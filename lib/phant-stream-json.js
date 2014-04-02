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

  this._writableState.objectMode = true;
  this._readableState.objectMode = false;

  this.id = id;

  this.file = options.hasOwnProperty('file') ? options.file : null;
  this.dir = options.hasOwnProperty('dir') ? options.dir : null;
  this.root = options.hasOwnProperty('root') ? options.root : path.join(__dirname, '..', 'tmp');
  this.cap = options.hasOwnProperty('cap') ? options.cap : (50 * 1024 * 1024);  // 50mb cap
  this.chunk = options.hasOwnProperty('chunk') ? options.chunk : (5 * 1024 * 1024); // 5mb chunks
  this.flags = options.hasOwnProperty('flags') ? options.flags : 'a+';
  this.mode = options.hasOwnProperty('mode') ? options.mode : 438; /*=0666*/

  if(! this.file) {
    this.getFilePath();
  }

  this.open();

  this.once('finish', this.close);

};

app.getFilePath = function() {

  var self = this;

  if(! this.dir) {
    this.getDir();
    return this.once('dir', function() {
      self.getFilePath();
    });
  }

  this.file = path.join(this.dir, 'stream.json');

  this.emit('path', this.file);

};

app.getDir = function() {

  var self = this;

  var dir = path.join(
    this.root,
    this.id.slice(0, 2),
    this.id.slice(2)
  );

  // ensure that the folder exists
  fs.exists(dir, function(exists) {

    if(exists) {
      self.dir = dir;
      return self.emit('dir', dir);
    }

    mkdirp(dir, function(err) {

      if(err) {
        self.emit('error', err);
      }

      self.dir = dir;
      self.emit('dir', dir);

    });

  });

};

app.remaining = function(cb) {

  var cap = this.cap;

  this.used(function(err, size) {

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

app.used = function(cb) {

  var self = this,
      total = 0;

  if(! this.dir) {
    return this.once('dir', function() {
      self.used(cb);
    });
  }

  fs.readdir(this.dir, function(err, files) {

    if(err) {
      return cb(err);
    }

    (function next() {

      var file = files.shift();

      if(! file) {
        return cb(null, total);
      }

      fs.stat(path.join(self.dir, file), function(err, st) {

        if(err) {
          return next();
        }

        total += st.size;

        next();

      });

    })();

  });

};

app.open = function() {

  var self = this;

  if(! this.file) {
    return this.once('path', function() {
      self.open();
    });
  }

  fs.open(this.file, this.flags, this.mode, function(err, fd) {

    if(err) {
      self.destroy();
      self.emit('error', err);
      return;
    }

    self.fd = fd;

    fs.fstat(fd, function(err, st) {

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

  if(typeof this.fd !== 'number') {

    return this.once('open', function() {
      self._write(data, encoding, cb);
    });

  }

  data = Buffer(JSON.stringify(data) + '\n');

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

  rotate(this.file, { compress: true, count: keep }, function(err, rotated) {

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

