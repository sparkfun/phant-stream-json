/**
 * phant-stream-json
 * https://github.com/sparkfun/phant-stream-json
 *
 * Copyright (c) 2014 SparkFun Electronics
 * Licensed under the GPL v3 license.
 */

'use strict';

/**** Module dependencies ****/
var _ = require('lodash'),
    events = require('events');

/**** phantStream prototype ****/
var app = {};

/**** Expose phantStream ****/
exports = module.exports = phantStream;

/**** Initialize a new phantStream ****/
function phantStream(config) {

  var storage = {};

  config = config || {};

  _.extend(storage, app);
  _.extend(storage, events.EventEmitter.prototype);
  _.extend(storage, config);

  return storage;

}

