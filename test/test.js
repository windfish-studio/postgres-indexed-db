'use strict';

//NOTE: due to the nature of this library, a sample database is necessary for testing. Find it by extracting
//test/test_db.zip... You will also have to edit dvdrental/restore.sql and replace $$PATH$$ with the path to the test DB
//directory. You should create a database called 'dvdrental_sample' and then import the sample data to that database.

var _ = require('lodash');
var pg = require('pg');
var exporter = require('../lib/index.js').Exporter;
var tape = require('tape');
var fs = require('fs');
var q = require('q');
var path = require('path');

var conf = {
    db: {
        database: "dvdrental_sample"
    },
    output_path: path.join(__dirname, '..', 'test/export_data'),
    results_per_page: 100
};

var export_promise = do_export();
var db_schema_promise = get_db_schema();

var db_schema, export_messages;

db_schema_promise.then(function (result) {
    db_schema = result.rows;
});

export_promise.then(function (messages) {
    export_messages = messages;
});

q.all([export_promise, db_schema_promise]).then(function () {

    var output_filenames = fs.readdirSync(path.join(__dirname, './export_data'));
    var manifest = require(path.join(__dirname, 'export_data', '_manifest.json'));

    tape("should export all database data in JSON format", function (t) {

        //get tables data from database
        var db_tables = [];
        _.each(db_schema, function (row) {
            if (row.table_schema == 'public' && row.table_type == 'BASE TABLE') {
                db_tables.push(row.table_name);
                t.equal(typeof manifest[row.table_name], 'object'); //manifest entry should exist for each table
            }
        });

        t.equals(output_filenames.length - 1, db_tables.length); //minus one for manifest

        _.each(db_tables, function (table_name) {
            var output_ar = require(path.join(__dirname, 'export_data', table_name + '.json'));
            t.equal(manifest[table_name].row_count, output_ar.length);
        });

        var pool = new pg.Pool(conf.db);
        pool.connect(function (err, client, done) {

            var query_promises = [];
            _.each(db_tables, function (table_name) {
                var query_def = q.defer();
                query_promises.push(query_def.promise)
                var output_ar = require(path.join(__dirname, 'export_data', table_name + '.json'));
                t.equal(manifest[table_name].row_count, output_ar.length);
                var random_item = output_ar[parseInt(Math.random()*1000) % manifest[table_name].row_count];

                var where_ar = [];
                _.each(manifest[table_name].primary_keys, function (pk_name) {
                    where_ar.push([pk_name, random_item[pk_name]].join(' = '));
                });

                pool.query("SELECT * FROM " + table_name + " WHERE " + where_ar.join(' AND '), []).then(function (result) {
                    var row = result.rows[0];
                    _.each(row, function (value, key) {
                        var _exp_v = random_item[key]
                        var _db_v = value;

                        if(value instanceof Date){
                            //some simple conversions for postgresql dates to javascript dates.
                            var psql_date = random_item[key];
                            if(psql_date.length == 10)
                                psql_date += ' 00:00:00';
                            var _d = new Date(psql_date.replace('T', ' '));

                            _exp_v = _d.getTime();
                            _db_v = value.getTime();
                        }

                        if(value instanceof Uint8Array){
                            //such a pain in the ass to test...
                            return;
                        }

                        t.ok(_exp_v == _db_v);
                    });
                    query_def.resolve();
                });
            });

            q.all(query_promises).then(function () {
                done();
                pool.end().then(function () {
                    t.end();
                });
            });
        });
    });

    tape("should emit progress messages", function (t) {
        var message_names = _.map(export_messages, function (msg_o) {
            return msg_o.message;
        });

        t.ok(message_names.length > 0);
        t.ok(message_names[message_names.length - 1] == 'success');
        t.ok(message_names.indexOf('progress') >= 0);
        t.ok(message_names.indexOf('table_finished') >= 0);

        t.end();
    });


});

function get_db_schema () {
    var def = q.defer();
    var pool = new pg.Pool(conf.db);
    pool.connect(function (err, client, done) {
        pool.query("SELECT table_name, table_schema, table_type FROM information_schema.tables", []).then(function (result) {
            def.resolve(result);
            done();
            pool.end();
        });

    });
    return def.promise;
};

function do_export () {

    var observable = exporter(conf);

    var def = q.defer();
    var messages = [];

    observable.subscribe(function(msg_o){
        messages.push(msg_o);
        switch(msg_o.message){
            case "success":
                console.log('finished');
                def.resolve(messages);
                break;
            case "progress":
                console.log("%"+(msg_o.value*100).toFixed(2)+" done...");
                break;
        }

    });

    return def.promise;
};