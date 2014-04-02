/**
 * phant-stream-json
 * https://github.com/sparkfun/phant-stream-json
 *
 * Copyright (c) 2014 SparkFun Electronics
 * Licensed under the GPL v3 license.
 */

'use strict';

/**** Module dependencies ****/
var mkdirp = require('mkdirp'),
    util = require('util'),
    path = require('path'),
    fs = require('fs');

/**** app prototype ****/
var app = {};

/**** Expose Helpers ****/
exports = module.exports = Helpers;

function Helpers(options) {

  var helpers = {};

  util._extend(helpers, app);
  util._extend(helpers, options);

  return helpers;

}

app.fileName = 'stream.json';
app.root = 'tmp';

app.filePath = function(id, page, cb) {

  var name = this.fileName;

  page = page || 0;

  // log files are numbered like this:
  // stream.json, stream.json.0, stream.json.1
  if(page > 1) {
    name = name + '.' + (page - 2);
  }

  this.directoryPath(id, function(err, dir) {

    if(err) {
      return cb(err);
    }

    cb(null, path.join(dir, name));

  });

};

app.directoryPath = function(id, cb) {

  var dir = path.join(
    this.root,
    id.slice(0, 2),
    id.slice(2)
  );

  // ensure that the folder exists
  fs.exists(dir, function(exists) {

    if(exists) {
      return cb(null, dir);
    }

    mkdirp(dir, function(err) {

      if(err) {
        return cb(err);
      }

      cb(null, dir);

    });

  });

};

app.files = function(id) {

  var reg = new RegExp(this.fileName);

  this.directoryPath(id, function(err, dir) {

    if(err) {
      return cb(err);
    }

    fs.readdir(dir, function(err, files) {

      if(err) {
        return cb(err);
      }

      var matched = [];

      for (i = 0, l = files.length; i < l; i++) {

        if (reg.test(files[i]) === true) {
          matched.push(path.join(dir, files[i]));
        }

      }

      cb(null, matched);

    });

  });

};

app.pageCount = function(id, cb) {

  this.files(id, function(err, files) {

    if(err) {
      return cb(err);
    }

    cb(null, files.length);

  });

};

app.remainingStorage = function(id, cap, cb) {

  this.used(id, function(err, size) {

    var remaining = 0;

    if(err) {
      return cb(err);
    }

    remaining = cap - size;

    if(remaining < 0) {
      remaining = 0;
    }

    cb(null, remaining);

  });

};

app.usedStorage = function(id, cb) {

  var total = 0;

  this.files(id, function(err, files) {

    if(err) {
      return cb(err);
    }

    (function next() {

      var file = files.shift();

      if(! file) {
        return cb(null, total);
      }

      fs.stat(file, function(err, st) {

        if(err) {
          return next();
        }

        total += st.size;

        next();

      });

    })();

  });

};

