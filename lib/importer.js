'use strict';

var oboe = require('oboe');
var Rx = require('rxjs');
var _ = require('lodash');
var q = require('q');

module.exports = function( data_path, idb_name ){
    var events = new Rx.Subject();

    var indexedDB = window.indexedDB || window.mozIndexedDB || window.webkitIndexedDB || window.msIndexedDB || window.shimIndexedDB;

    var init = function () {
        //retreive manifest
        oboe(data_path + '/_manifest.json')
            .done(function(manifest) {
                events.next({
                    message: "manifest",
                    value: manifest
                });


                var delReq = indexedDB.deleteDatabase(idb_name);

                delReq.onblocked = function(){
                    console.log('database delete blocked');
                };

                var fillIDB = function () {
                    var open = indexedDB.open(idb_name, 1);

                    open.onerror = function(event) {
                        console.log('error opening indexedDB');
                    };


                    var migrate = function(event) {
                        var db = event.target.result;
                        var store_promises = [];
                        _.each(manifest, function (_manifest, key) {
                            var store = db.createObjectStore(key, {keyPath: _manifest.primary_key});

                            _.each(_manifest.indices, function(columns, idx_name){
                                store.createIndex(idx_name, columns);
                            });

                            var store_deferred = q.defer();
                            store_promises.push(store_deferred.promise);

                            store.transaction.addEventListener('complete', function(){

                                var stored_rows = 0;

                                oboe(data_path + '/' + key + '.json')
                                    .node({
                                        '!.*': function(row){
                                            var tx = db.transaction([key], "readwrite");
                                            var _store = tx.objectStore(key);

                                            //load each row into indexedDB
                                            var _result = _store.put(row);

                                            _result.onsuccess = function(){
                                                stored_rows += 1;

                                                if(stored_rows == _manifest.row_count){
                                                    store_deferred.resolve();
                                                }
                                            };

                                            _result.onerror = function () {
                                                store_deferred.reject();
                                            };

                                            return oboe.drop; //allows to read files bigger than available RAM
                                        }
                                    });


                            });

                        });

                        q.all(store_promises).then(function () {
                            db.close();
                        });

                    };

                    open.onupgradeneeded = migrate;
                };

                delReq.onsuccess = fillIDB;
                delReq.onerror = fillIDB;

            });
    };

    init();

    return events;
};