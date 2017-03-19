'use strict';

//NOTE: due to the nature of this library, a sample database is necessary for testing. Find it by extracting
//test/test_db.zip... You will also have to edit dvdrental/restore.sql and replace $$PATH$$ with the path to the test DB
//directory. You should create a database called 'dvdrental_sample' and then import the sample data to that database.

var _ = require('lodash');
var pg = require('pg');
var exporter = require('../lib/index.js');
var tape = require('tape');
var fs = require('fs');
var path = require('path');

var conf = {
    db: {
        database: "dvdrental_sample"
    },
    output_path: "test/export_data",
    results_per_page: 100
};

tape("should export all database data to specified directory", function (t) {

    var observable = exporter(conf);

    var pool = new pg.Pool(conf.db);
    pool.connect(function (err, client, done){

        observable.subscribe(function(msg_o){
            switch(msg_o.message){
                case "finished":
                    console.log('finished');

                    var output_filenames = fs.readdirSync(path.join(__dirname, './export_data'));
                    var manifest = require(path.join(__dirname, 'export_data', '_manifest.json'));

                    //get tables data from database
                    pool.query("SELECT table_name, table_schema, table_type FROM information_schema.tables", []).then(function(result){
                        var db_tables = [];
                        _.each(result.rows, function(row){
                            if(row.table_schema == 'public' && row.table_type == 'BASE TABLE'){
                                db_tables.push(row.table_name);
                                t.equal(typeof manifest[row.table_name], 'object'); //manifest entry should exist for each table
                            }
                        });

                        t.equals(output_filenames.length - 1, db_tables.length); //minus one for manifest

                        _.each(db_tables, function(table_name){
                            var output_ar = require(path.join(__dirname, 'export_data', table_name+'.json'));
                            t.equal(manifest[table_name].row_count, output_ar.length);
                        });

                        done();
                        pool.end();

                        t.end();
                    });

                    break;
                case "progress":
                    console.log("%"+(msg_o.value*100).toFixed(2)+" done...");
                    break;
            }

        });

    });
});

