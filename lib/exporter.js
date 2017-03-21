var pg = require('pg');
var _ = require('lodash');
var q = require('q');
var fs = require('fs');
var path = require('path');
var mkdirp = require('mkdirp');
var del = require('del');
var Rx = require('rxjs');

module.exports = function(conf){
    var pool = new pg.Pool(conf.db);

    var tables = [];
    var tables_sizes = {};
    var primary_keys = {}; // key => string
    var table_indices = {}; // key => array
    var count_promises = [];
    var results_per_page = conf.results_per_page || 5000;
    var output_promises = [];
    var events = new Rx.Subject();
    var total_pages_count = 0;
    var current_pages_written = 0;

    var progressTimer = Rx.Observable.interval(1000);
    var subscription = progressTimer.subscribe(function(){
        var progress = current_pages_written / total_pages_count;

        if(isNaN(progress)){
            progress = 0;
        }

        events.next({
            message: "progress",
            value: progress
        });

        events.subscribe(function (msg_o) {
           if(msg_o.message == 'finished'){
               subscription.unsubscribe();
           }
        });
    });

    var indices_query = fs.readFileSync(path.join(__dirname, 'queries', 'fetch_indices.sql'), 'utf8');

    var outpath = path.join(__dirname, '..', (conf.output_path || 'database_json'));
    del.sync([path.join(outpath, "*")]);
    mkdirp.sync(outpath);

    // connect to our database
    pool.connect(function (err, client, done) {

        // execute a query on our database
        pool.query("SELECT table_name, table_schema, table_type FROM information_schema.tables", []).then(function(result){
            _.each(result.rows, function(row){
                if(row.table_schema == 'public' && row.table_type == 'BASE TABLE'){
                    tables.push(row.table_name);
                }
            });

            _.each(tables, function(table_name){
                var count_promise = pool.query("SELECT COUNT(*) FROM "+table_name);

                count_promises.push(count_promise);
                count_promise.then(function(count_res){
                    tables_sizes[table_name] = parseInt(count_res.rows[0].count);
                }, function(err){
                    throw err;
                });

            });

            //get DB index/primary_key data...
            var indices_promise = pool.query(indices_query);

            indices_promise.then(function(index_res){
                _.each(index_res.rows, function (row) {
                    if(row.index_name.indexOf('pkey') >= 0){
                        primary_keys[row.table_name] = primary_keys[row.table_name] || []
                        primary_keys[row.table_name].push(row.column_name);
                    }

                    if(row.index_name.substr(0,3) == 'idx'){
                        table_indices[row.table_name] = table_indices[row.table_name] || {};
                        table_indices[row.table_name][row.index_name] = table_indices[row.table_name][row.index_name] || [];
                        table_indices[row.table_name][row.index_name].push(row.column_name);
                    }


                });
            });

            q.all(count_promises.concat(indices_promise)).then(function(){

                //write manifest...
                var manifest_wstream = fs.createWriteStream(path.join(outpath, '_manifest.json'));

                manifest_wstream.write("{");
                _.each(tables, function(table_name, idx){
                    if(idx != 0){
                        manifest_wstream.write('\n,');
                    }
                    manifest_wstream.write('\n' + [JSON.stringify(table_name), JSON.stringify({
                            name: table_name,
                            row_count: tables_sizes[table_name],
                            indices: table_indices[table_name],
                            primary_keys: primary_keys[table_name]
                        })].join(': '));
                });

                var manifest_def = q.defer();
                manifest_wstream.write("\n}");
                manifest_wstream.end();


                manifest_wstream.on('close', manifest_def.resolve);


                output_promises.push(manifest_def.promise);

                _.each(tables, function(table_name){
                    var output_def = q.defer();
                    output_promises.push(output_def.promise);
                    var pages = parseInt(tables_sizes[table_name] / results_per_page);
                    if(tables_sizes[table_name] % results_per_page != 0){
                        pages += 1;
                    }

                    total_pages_count += pages;

                    var pages_promises = [];
                    var pages_defs = [];

                    _.each(new Array(pages), function () {
                        var page_def = q.defer();
                        pages_promises.push(page_def.promise);
                        pages_defs.push(page_def);

                        page_def.promise.then(function () {
                            current_pages_written += 1;
                        });
                    });

                    var withPage = function(page){
                        //stop case
                        if(page == pages){
                            return;
                        }

                        var query =
                            "SELECT row_to_json(d) FROM (SELECT * FROM " + table_name + ") AS d " +
                            "LIMIT " + results_per_page + " OFFSET " + (page * results_per_page) + ";";
                        pool.query(query).then(function (res) {
                            var outfile_path = path.join(outpath, table_name+'.json');
                            var outfile_exists = fs.existsSync(outfile_path);
                            var outfile_flags = 'w';
                            var outfile_size = 0;

                            if(outfile_exists){
                                var outfile_stats = fs.statSync(outfile_path);
                                outfile_size = outfile_stats.size;
                                outfile_flags = 'r+';
                            }

                            var wstream = fs.createWriteStream(outfile_path, {
                                flags: outfile_flags,
                                start: outfile_size
                            });

                            if(page == 0){
                                wstream.write("[\n");
                            }

                            _.each(res.rows, function(row, row_idx){
                                if(row_idx != 0 || page > 0){
                                    wstream.write(',\n');
                                }
                                wstream.write(JSON.stringify(row.row_to_json));
                            });

                            if(page == pages - 1){
                                wstream.write("\n]");
                            }

                            wstream.end();
                            wstream.on('close', function () {
                                pages_defs[page].resolve();
                                return withPage(page + 1);
                            });

                        }, function (err) {
                            throw err;
                        });
                    };

                    withPage(0);

                    q.all(pages_promises).then(function(){
                        output_def.resolve();
                        events.next({
                            message: "table_finished",
                            value: table_name
                        });
                    });
                });

                q.all(output_promises).then(function(){
                    done();
                    pool.end().then(function(){
                        events.next({message: "finished"});
                    });
                });
            });
        });

    });

    return events;
};