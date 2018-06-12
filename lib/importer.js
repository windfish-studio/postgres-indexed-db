'use strict';

var ndjsonStream = require('can-ndjson-stream');
var Subject = require('rxjs/Subject').Subject;
var _ = {
    each: require('lodash/each'),
    map: require('lodash/map'),
    extend: require('lodash/extend')
};
var q = require('q');

module.exports.import = function( data_path, open_db, options ){
    var events = new Subject();

    if(options === undefined){
        options = {};
    }

    if(options.items_per_page === undefined){
        options.items_per_page = 10000;
    }

    //retreive manifest
    fetch(data_path + '/_manifest.json')
        .then(function(response) {
            if(!response.ok){
                return events.next({
                    message: "error",
                    value: "manifest not found"
                });
            }

            return response.json();
        }).then(function(manifest){
            open_db(function (event) {
                var db = event.target.result;
                events.next({
                    message: "manifest",
                    value: manifest
                });

                var pages_written = 0;
                var total_pages = 0;


                var reportProgress = function(progress){
                    events.next({
                        message: "progress",
                        value: progress
                    });
                };

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
                    var store_opts = {};

                    if(_manifest.row_count == 0){
                        return;
                    }

                    if(_manifest.primary_keys !== undefined){
                        store_opts.keyPath = _manifest.primary_keys[0];

                        if(_manifest.primary_keys.length > 1){
                            pkey_indices["idx_multiple_pkeys"] = _manifest.primary_keys;
                            delete store_opts.keyPath;
                        }
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


                        fetch(data_path + '/' + key + '.ndjson')
                            .then(function(response){

                                if(!response.ok){
                                    return failout('oboe_read_failed');
                                }

                                return ndjsonStream(response.body);
                            }).then(function(rowsStream){
                                var reader = rowsStream.getReader();
                                var doRead = function (result) {
                                    if (result.done) return;
                                    var row = result.value;

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

                                    reader.read().then(doRead);
                                };

                                reader.read().then(doRead);

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
            });
        });

    return events;
};