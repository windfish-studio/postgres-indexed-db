'use strict';

if(typeof window == 'undefined'){
  var exporter_path = './exporter.js'; //excludes from browserify
  module.exports = require(exporter_path);
}else{
  //global
  module.exports = window.PGIndexedDBImporter = require('./importer.js');
}