'use strict';

var phantStream = require('../index.js');

exports.phantStream = {
  setUp: function(done) {
    done();
  },
  'no args': function(test) {
    test.expect(1);
    test.ok(phantStream, 'should be ok');
    test.done();
  }
};
