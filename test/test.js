'use strict';

//NOTE: due to the nature of this library, a sample database is necessary for testing. Find it by extracting
//test/test_db.zip... You will also have to edit dvdrental/restore.sql and replace $$PATH$$ with the path to the test DB
//directory. You should create a database called 'dvdrental_sample' and then import the sample data to that database.

var _ = require('lodash');
var pg = require('pg');
var exporter = require('../lib/index.js');
var tape = require('tape');
var fs = require('fs');
var q = require('q');
var path = require('path');

var conf = {
    db: {
        database: "dvdrental_sample"
    },
    output_path: "test/export_data",
    results_per_page: 100
};

var export_promise = do_export();
var db_schema_promise = get_db_schema();

var db_schema;

db_schema_promise.then(function (result) {
    db_schema = result.rows;
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

    observable.subscribe(function(msg_o){
        switch(msg_o.message){
            case "finished":
                console.log('finished');
                def.resolve();
                break;
            case "progress":
                console.log("%"+(msg_o.value*100).toFixed(2)+" done...");
                break;
        }

    });

    return def.promise;
};