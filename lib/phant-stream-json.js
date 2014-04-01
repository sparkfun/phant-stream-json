/**
 * phant-stream-json
 * https://github.com/sparkfun/phant-stream-json
 *
 * Copyright (c) 2014 SparkFun Electronics
 * Licensed under the GPL v3 license.
 */

'use strict';

/**** Module dependencies ****/
var Duplex = require('stream').Duplex,
    mkdirp = require('mkdirp'),
    rotate = require('log-rotate'),
    path = require('path'),
    util = require('util'),
    fs = require('fs');

/**** Make PhantStream a Duplex stream ****/
util.inherits(PhantStream, Duplex);

/**** PhantStream prototype ****/
var app = PhantStream.prototype;

/**** Expose PhantStream ****/
exports = module.exports = PhantStream;

/**** Initialize a new PhantStream ****/
function PhantStream(id, options) {

  if (! (this instanceof PhantStream)) {
    return new PhantStream(id, options);
  }

  options = options || {};

  Duplex.call(this, options);

  this.id = id;

  this.path = options.hasOwnProperty('path') ? options.path : null;  // 50mb cap
  this.cap = options.hasOwnProperty('cap') ? options.cap : (50 * 1024 * 1024);  // 50mb cap
  this.chunk = options.hasOwnProperty('chunk') ? options.chunk : (5 * 1024 * 1024); // 5mb chunks
  this.flags = options.hasOwnProperty('flags') ? options.flags : 'a+';
  this.root = options.hasOwnProperty('root') ? options.root : path.join(__dirname, '..', 'tmp');
  this.mode = options.hasOwnProperty('mode') ? options.mode : 438; /*=0666*/

  if(! this.path) {
    this.getPath();
  }

  this.open();

  this.once('finish', this.close);

};

app.getPath = function() {

  var self = this,
      dir = [
        this.root,
        this.id.slice(0, 2),
        this.id.slice(2)
      ];

  // ensure that the folder exists
  mkdirp(path.join.apply(null, dir), function (err) {

    if(err) {
      self.emit('error', err);
    }

    // append file name
    dir.push('stream.json');

    self.path = path.join.apply(null, dir)

    self.emit('path', self.path);

  });

};

app.open = function() {

  var self = this;

  if(! this.path) {
    return this.once('path', function() {
      self.open();
    });
  }

  fs.open(this.path, this.flags, this.mode, function(err, fd) {

    if (err) {
      self.destroy();
      self.emit('error', err);
      return;
    }

    self.fd = fd;

    fs.stat(self.path, function(err, st) {

      if(err) {
        return this.emit('error', err);
      }

      self.size = st.size;
      self.emit('open', fd);

    });

  });

};

app._write = function(data, encoding, cb) {

  var self = this;

  if (! Buffer.isBuffer(data))
    return this.emit('error', new Error('Invalid data'));

  if(typeof this.fd !== 'number') {

    return this.once('open', function() {
      self._write(data, encoding, cb);
    });

  }

  if (this.size + data.length > this.chunk) {
    this.rotate();
    this.once('open', function() {
      self.writeData(data, cb);
    });
  } else {
    this.writeData(data, cb);
  }

};

app.writeData = function(data, cb) {

  var self = this;

  fs.write(this.fd, data, 0, data.length, this.pos, function(err, bytes) {

    if(err) {
      self.close();
      return cb(er);
    }

    self.size += bytes;
    cb();

  });

};

app.rotate = function() {

  var self = this,
      keep = Math.floor(this.cap / this.chunk);

  // close the file
  this.close();

  rotate(this.path, { compress: true, count: keep }, function(err, rotated) {

    if(err) {
      return self.emit('error', err);
    }

    self.open();

  });

};

app.close = function() {

  var self = this;

  fs.close(this.fd, function(err) {

    if(err) {
      self.emit('error', err);
    }

  });

};

