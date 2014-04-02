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
    events = require('events');
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
  helpers = helpers({root: this.root});

}

app.root = 'tmp';

