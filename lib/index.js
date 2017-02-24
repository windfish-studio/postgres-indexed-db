'use strict';

var exporter_path = './exporter.js'; //excludes from browserify
module.exports = {
  importer: require('./importer.js'),
  exporter: require(exporter_path)
};