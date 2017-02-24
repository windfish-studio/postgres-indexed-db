'use strict';

var gulp = global.gulp = require('gulp');

require("./gulp/browserify");

gulp.task('default', ['pg-to-json']);