'use strict';

var oboe = require('oboe');
var Rx = require('rxjs');
var _ = require('lodash');
var q = require('q');

module.exports = function( data_path, idb_name, options ){
    var events = new Rx.Subject();

    if(options === undefined){
        options = {};
    }

    if(options.items_per_page === undefined){
        options.items_per_page = 10000;
    }

    var indexedDB = window.indexedDB || window.mozIndexedDB || window.webkitIndexedDB || window.msIndexedDB || window.shimIndexedDB;

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
                var pages_written = 0;
                var total_pages = 0;

                var open = indexedDB.open(idb_name, 1);

                open.onerror = function(event) {
                    console.log('error opening indexedDB');
                };

                var reportProgress = function(progress){
                    events.next({
                        message: "progress",
                        value: progress
                    });
                };


                var migrate = function(event) {
                    var db = event.target.result;
                    var store_promises = [];

                    var failout = function(reason){
                        events.next({
                            message: "error",
                            value: reason
                        });

                        db.close();
                    };

                    _.each(manifest, function (_manifest, key) {

                        var pkey_indices = {};
                        var store_opts = {
                            keyPath: _manifest.primary_keys[0]
                        };

                        if(_manifest.primary_keys.length > 1){
                            pkey_indices["idx_multiple_pkeys"] = _manifest.primary_keys;
                            delete store_opts.keyPath;
                        }

                        var store = db.createObjectStore(key,  store_opts);

                        _.each(_.extend(pkey_indices, _manifest.indices), function(columns, idx_name){
                            store.createIndex(idx_name, columns);
                        });

                        var store_deferred = q.defer();
                        store_promises.push(store_deferred.promise);

                        var pages = parseInt(_manifest.row_count / options.items_per_page);

                        if(_manifest.row_count % options.items_per_page != 0){
                            pages += 1;
                        };

                        total_pages += pages;

                        var page_promises = new Array(pages);
                        var page_deferreds = new Array(pages);

                        _.each(page_promises, function(unused, i){
                            var d = q.defer();
                            page_deferreds[i] = d;
                            page_promises[i] = d.promise;

                            d.promise.then(function () {
                                pages_written += 1;
                                reportProgress(pages_written / total_pages);
                            }, function () {
                                failout('page_storage_failed');
                            });
                        });

                        q.all(page_promises).then(store_deferred.resolve, store_deferred.reject);

                        store.transaction.addEventListener('complete', function(){

                            var current_page = 0;
                            var page_items = [];

                            var store_page = function(_page, _items){
                                var tx = db.transaction([key], "readwrite");
                                var _store = tx.objectStore(key);

                                _.each(_items, function (item) {
                                    var _result;

                                    if(_manifest.primary_keys.length > 1){
                                        var multiKey = _.map(_manifest.primary_keys, function (pkey) {
                                            return item[pkey];
                                        }).join('_');
                                        _result = _store.put(item, multiKey);
                                    }else{
                                        _result = _store.put(item);
                                    }

                                    _result.onerror = function () {
                                        page_deferreds[_page].reject();
                                    };
                                });

                                tx.addEventListener('complete', function () {
                                    page_deferreds[_page].resolve();
                                });
                            };


                            oboe(data_path + '/' + key + '.json')
                                .node({
                                    '!.*': function(row){
                                        page_items.push(row);

                                        if(page_items.length >= options.items_per_page ||
                                            (
                                                current_page == (pages - 1) &&
                                                page_items.length >= (_manifest.row_count % options.items_per_page)
                                            )
                                        ){
                                            store_page(current_page, page_items);
                                            page_items = [];
                                            current_page += 1;
                                        }

                                        return oboe.drop; //allows to read files bigger than available RAM
                                    }
                                })
                                .fail(function(){
                                    failout('oboe_read_failed');
                                });


                        });

                        store.transaction.addEventListener('error', function(){
                            failout('idb_schema_failed');
                        });

                    });

                    q.all(store_promises).then(function () {
                        events.next({
                            message: "success"
                        });
                        db.close();
                    });

                };

                open.onupgradeneeded = migrate;
            };

            delReq.onsuccess = fillIDB;
            delReq.onerror = fillIDB;

        });

    return events;
};