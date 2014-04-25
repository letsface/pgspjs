'use strict';

var sp = require('../sp.js');
var spConfig = require('./spconfig.js').newConfig();
var Q = require('q');
var fs = require('fs');

describe(__filename, function() {
  spConfig.beforeLoad = function(connection) {
    return Q
      .nfapply(fs.readFile, [__dirname + '/initialization.sql', 'utf8'])
      .then(function(setupSql) {
        return connection.queryInTransaction(setupSql);
      });
  }

  spConfig.beforeEnd = function(connection) {
    return Q
      .nfapply(fs.readFile, [
        __dirname + '/initialization-teardown.sql',
        'utf8'
      ])
      .then(function(teardownSql) {
        return connection.queryInTransaction(teardownSql);
      });
  }

  it('onConnection is called once on connection', function(done) {
    var sps;
    var count = 0;
    spConfig.onConnection = function(connection) {
      expect(connection.client.poolCount).toEqual(1);
      count++;
      return Q();
    }

    Q()
      .then(function() {
        return sp.exportStoredProcs(spConfig);
      })
      .then(function(s) {
        sps = s;
      })
      .then(function() {
        return sps
          .transaction()
          .test_function()
          .promiseNoData();
      })
      .then(function() {
        if (count > 1)
          throw new Error('called multiple time on the same client: ' + count);
      })
      .then(function() {
        return sps.end();
      })
      .then(done)
      .fail(done)
      .done();
  });
});