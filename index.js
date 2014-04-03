/**
 * phant-stream-json
 * https://github.com/sparkfun/phant-stream-json
 *
 * Copyright (c) 2014 SparkFun Electronics
 * Licensed under the GPL v3 license.
 */

'use strict';

/**** Module dependencies ****/
var util = require('util'),
    events = require('events'),
    async = require('async'),
    helpers = require('./lib/helpers'),
    Readable = require('./lib/readable'),
    Writable = require('./lib/writable');

/**** Make PhantStream an event emitter ****/
util.inherits(PhantStream, events.EventEmitter);

/**** app prototype ****/
var app = PhantStream.prototype;

/**** Expose PhantStream ****/
exports = module.exports = PhantStream;

function PhantStream(options) {

  if (! (this instanceof PhantStream)) {
    return new PhantStream(options);
  }

  options = options || {};

  events.EventEmitter.call(this, options);

  // apply the options
  util._extend(this, options);

  // point the file helpers at passed root folder
  this.helpers = helpers({root: this.root});

}

app.name = 'phant json stream';
app.cap = 50 * 1024 * 1024; // 50mb
app.chunk = 500 * 1024; // 500k
app.root = 'tmp';

app.readStream = function(id, page) {

  var all = false;

  if(! page) {
    all = true;
    page = 1;
  }

  return new Readable(id, {
    page: page,
    all: all,
    root: this.root
  });

};

app.write = function(id, data) {

  var stream = this.writeStream(id);

  stream.end(JSON.stringify(data) + '\n');

};

app.writeStream = function(id) {

  return new Writable(id, {
    cap: this.cap,
    chunk: this.chunk,
    root: this.root
  });

};

app.stats = function(id, cb) {

  var self = this;

  var stats = {
    pageCount: 0,
    remaining: 0,
    used: 0,
    cap: this.cap
  };

  async.parallel([
    function(callback) {
      self.helpers.usedStorage(id, function(err, used) {

        if(err) {
          callback(err);
        }

        stats.used = used;

        callback();

      });
    },
    function(callback) {
      self.helpers.pageCount(id, function(err, count) {

        if(err) {
          callback(err);
        }

        stats.pageCount = count;

        callback();

      });
    }
  ], function(err) {

      if(err) {
        cb(err);
      }

      stats.remaining = stats.cap - stats.used;

      if(stats.remaining < 0)
        stas.remaining = 0;

      cb(null, stats);

  });

};
