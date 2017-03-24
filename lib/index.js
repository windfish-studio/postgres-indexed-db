'use strict';

var master_obj =  {}

var is_browser = (typeof window != 'undefined' || typeof WorkerGlobalScope != 'undefined');

if(!is_browser){
  var exporter_path = './exporter.js'; //excludes from browserify
  master_obj.exporter = require(exporter_path);
}else{
  if(typeof window != 'undefined')
    window.PGIndexedDB = master_obj;
  master_obj.importer = require('./importer.js');
}

module.exports = master_obj;