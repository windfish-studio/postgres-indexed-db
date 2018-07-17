'use strict';

var ndjsonStream = require('can-ndjson-stream');
var moment = require('moment');
var Subject = require('rxjs/Subject').Subject;

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

                Object.entries(manifest).forEach(function (_o) {
                    var _manifest = _o[1], key = _o[0];

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

                    var all_indices = Object.assign(pkey_indices, _manifest.indices);
                    Object.entries(all_indices).forEach(function(_o){
                        var columns = _o[1], idx_name = _o[0];
                        store.createIndex(idx_name, columns);
                    });

                    var store_deferred = q.defer();
                    store_promises.push(store_deferred.promise);

                    var pages = parseInt(_manifest.row_count / options.items_per_page);

                    if(_manifest.row_count % options.items_per_page != 0){
                        pages += 1;
                    };

                    total_pages += pages;

                    var page_promises = new Array(pages).fill(undefined);
                    var page_deferreds = new Array(pages).fill(undefined);

                    page_promises.forEach(function(unused, i){
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

                            _items.forEach(function (item) {
                                var _result;

                                if(_manifest.primary_keys.length > 1){
                                    var multiKey = Object.entries(_manifest.primary_keys).map(function (_o) {
                                        return item[_o[1]];
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

                                    Object.entries(row).forEach(function (_o) {
                                        var _v = _o[1], _k = _o[0];
                                        var col_data_type = _manifest.columns[_k];
                                        switch(col_data_type){
                                            case 'date':
                                            case 'timestamp with timezone':
                                            case 'timestamp without timezone':
                                                row[_k] = moment(_v).toDate();
                                                break;
                                            case 'bytea':
                                                if (_v == null)
                                                    break;
                                                var hex_string = _v.slice(2);
                                                row[_k] = new ArrayBuffer(hex_string.length / 2);
                                                var uint8view = new Uint8Array(row[_k]);
                                                var current_byte_idx = 0;
                                                while(hex_string.length > 0){
                                                    var hex_pair = hex_string.slice(0,2);
                                                    hex_string = hex_string.slice(2);
                                                    uint8view[current_byte_idx] = parseInt(hex_pair, 16);
                                                    current_byte_idx++;
                                                }
                                                break;
                                        }

                                    });

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