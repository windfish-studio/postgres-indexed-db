'use strict';

//NOTE: due to the nature of this library, a sample database is necessary for testing. Find it by extracting
//test/test_db.zip... You will also have to edit dvdrental/restore.sql and replace $$PATH$$ with the path to the test DB
//directory. You should create a database called 'dvdrental_sample' and then import the sample data to that database.

var _ = require('lodash');
var pg = require('pg');
var exporter = require('../lib/index.js').exporter;
var tape = require('tape');

tape("should export all database data to specified directory", function (t) {
    var observable = exporter({
        db: {
            database: "dvdrental_sample"
        },
        results_per_page: 100
    });

    observable.subscribe(function(msg_o){
        if(msg_o.message == "finished"){
            t.end();
            console.log('finished');
        }else if(msg_o.message == "table_finished"){
            console.log('\''+msg_o.value+'\' write finished...');
        }
    });
});

