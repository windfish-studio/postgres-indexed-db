var pg = require('pg');
var _ = require('lodash');
var q = require('q');
var fs = require('fs');
var path = require('path');
var mkdirp = require('mkdirp');
var del = require('del');
var Rx = require('rxjs');
var gutil = require('gulp-util');

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
    var schema = conf.schema;

    var progressTimer = Rx.Observable.interval(1000);
    var timerSubscription = progressTimer.subscribe(function(){
        var progress = current_pages_written / total_pages_count;

        if(isNaN(progress)){
            progress = 0;
        }

        events.next({
            message: "progress",
            value: progress
        });

        events.subscribe(function (msg_o) {
           if(msg_o.message == 'success'){
               timerSubscription.unsubscribe();
           }
        });
    });

    var indices_query = fs.readFileSync(path.join(__dirname, 'queries', 'fetch_indices.sql'), 'utf8');

    var outpath = conf.output_path;

    if(outpath === undefined){
        throw "output_path must be passed in options";
    }

    del.sync([path.join(outpath, "*")]);
    mkdirp.sync(outpath);

    var error_out = function(e){
        var error_obj = {message: 'error'};
        if(e)
            error_obj.value = e;

        events.next(error_obj);
        timerSubscription.unsubscribe();
    };

    // connect to our database
    pool.connect(function (err, client, done) {

        if(err instanceof Error)
            return error_out(err);

        // execute a query on our database
        client.query("SELECT table_name, table_schema, table_type FROM information_schema.tables", [])
        .then(function(result) {
            _.each(result.rows, function (row) {
                if (row.table_type == 'BASE TABLE' &&
                    row.table_schema != 'pg_catalog' &&
                    row.table_schema != 'information_schema' &&
                    row.table_schema == (schema || row.table_schema) ) {
                    var qualified_name = (schema)? row.table_name : [row.table_schema, row.table_name].join('.');
                    tables.push(qualified_name);
                }
            });
        }).then(function() {
            _.each(tables, function (table_name) {
                count_promises.push(client.query("SELECT COUNT(*) FROM " + table_name).then(function (count_res) {
                    tables_sizes[table_name] = parseInt(count_res.rows[0].count);
                }, function (err) {
                    throw err;
                }));
            });

            return q.all(count_promises);
        }).then(function() {
            return client.query(indices_query).then(function (index_res) {
                _.each(index_res.rows, function (row) {
                    var _tname = (schema)? row.table_name : [row.table_schema, row.table_name].join('.');
                    if (row.idx_type == 'PRIMARY KEY') {
                        primary_keys[_tname] = primary_keys[_tname] || [];
                        primary_keys[_tname].push(row.column_name);
                    } else {
                        table_indices[_tname] = table_indices[_tname] || {};
                        table_indices[_tname][row.index_name] = table_indices[_tname][row.index_name] || [];
                        table_indices[_tname][row.index_name].push(row.column_name);
                    }
                });
            });
        })
        .then(function () {
            return client.query(`
                SELECT *
                FROM information_schema.columns
                ${schema ? `WHERE table_schema = '${schema}'` : ''}
            `).then(function (result) {
                var table_columns = {};
                result.rows.forEach(function (_r) {
                    var _tname = (schema)? _r.table_name : [_r.table_schema, _r.table_name].join('.');
                    table_columns[_tname] = table_columns[_tname] || {};
                    table_columns[_tname][_r.column_name] = _r.data_type;
                });
                return table_columns
            });
        })
        .then(function(table_columns) {
            //write manifest...
            var manifest_wstream = fs.createWriteStream(path.join(outpath, '_manifest.json'));

            manifest_wstream.write("{");
            _.each(tables, function (table_name, idx) {
                if (idx != 0) {
                    manifest_wstream.write('\n,');
                }
                manifest_wstream.write('\n' + [JSON.stringify(table_name), JSON.stringify({
                    name: table_name,
                    row_count: tables_sizes[table_name],
                    indices: table_indices[table_name],
                    primary_keys: primary_keys[table_name],
                    columns: table_columns[table_name]
                })].join(': '));
            });

            var manifest_def = q.defer();
            manifest_wstream.write("\n}");
            manifest_wstream.end();

            manifest_wstream.on('close', manifest_def.resolve);

            return manifest_def.promise;
        }).then(function () {

            _.each(tables, function(table_name){

                if(tables_sizes[table_name] == 0)
                    return;

                var output_def = q.defer();
                output_promises.push(output_def.promise);
                var pages = parseInt(tables_sizes[table_name] / results_per_page);
                if(tables_sizes[table_name] % results_per_page != 0){
                    pages += 1;
                }

                total_pages_count += pages;


                var withPage = function(page){
                    //stop case
                    if(page == pages){
                        return;
                    }

                    var query =
                        "SELECT row_to_json(d) FROM (SELECT * FROM " + table_name + ") AS d " +
                        "ORDER BY "+primary_keys[table_name].join(', ') + " " +
                        "LIMIT " + results_per_page + " OFFSET " + (page * results_per_page) + ";";
                    return client.query(query).then(function (res) {
                        var outfile_path = path.join(outpath, table_name+'.ndjson');
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

                        var i = res.rows.length;
                        var _def = q.defer();

                        function write() {
                            var encoding = 'utf8';
                            var ok = true;
                            do {
                                i--;

                                var data_o = res.rows[i].row_to_json;
                                var data = "";

                                if(i != (res.rows.length-1) || page > 0)
                                    data += ('\n');
                                data += JSON.stringify(data_o);

                                if (i === 0) {
                                    // last time!
                                    wstream.write(data, encoding, _def.resolve);
                                } else {
                                    // see if we should continue, or wait
                                    // don't pass the callback, because we're not done yet.
                                    ok = wstream.write(data, encoding);
                                }

                            } while (i > 0 && ok);
                            if (i > 0) {
                                // had to stop early!
                                // write some more once it drains
                                wstream.once('drain', write);
                            }
                        }

                        write();

                        wstream.on('close', _def.resolve);

                       return _def.promise.then(function(){
                           wstream.end();
                           current_pages_written += 1;
                           return withPage(page + 1);
                       });

                    }, error_out);
                };

                return withPage(0).then(function(){
                    output_def.resolve();
                    events.next({
                        message: "table_finished",
                        value: table_name
                    });
                }, error_out);

            });

            return q.all(output_promises)
        }, error_out)
        .then(function(){
            client.release();
            pool.end().then(function(){
                events.next({message: "success"});
            });
            done();

        }, error_out);

    });

    return events;
};