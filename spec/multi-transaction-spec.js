'use strict';

var sp = require('../sp.js');
var fs = require('fs');
var spconfig = require('./spconfig.js');
var Q = require('q');

describe(__filename, function() {
  // console.log(JSON.stringify(spconfig));

  beforeEach(function(done) {
    var self = this;
    var spConfig = spconfig.newConfig();
    spConfig.beforeLoad = function(connection) {
      return Q
        .nfapply(fs.readFile, [__dirname + '/multi-transaction.sql', 'utf8'])
        .then(function(setupSql) {
          return connection.queryInTransaction(setupSql);
        });
    }

    Q()
      .then(function() {
        return sp.exportStoredProcs(spConfig);
      })
      .then(function(s) {
        self.sps = s;
      })
      .then(done)
      .fail(done)
      .done();
  });

  it('large number of transactions work to completion', function(done) {
    var self = this;
    var promises = [];

    var TRANSACTION_COUNT = 100;
    var assertCount = 0;
    var callCount = 0;
    var transactionCount = 0;
    for (var i = 0; i < TRANSACTION_COUNT; i++) {
      var promise = Q()
        .then(function() {
          var id = callCount++;
          return self.sps
            .transaction(null, 'chain #' + id)
            .chain(function() {
              transactionCount++;
            })
            .create_entity('create', {})
            .store('create')
            .query_entity('query', {})
            .store('query')
            .update_entity('update', {})
            .store('update')
            .remove_entity('remove', {})
            .store('remove')
            .retrieveStore()
            .promiseData();
        })
        .then(function(store) {
          assertCount++;
          expect(store).toEqual({
            create: {},
            query: {},
            update: {},
            remove: {}
          });
          return store;
        });

      promises.push(promise);
    }

    Q.all(promises)
      .then(function(results) {
        expect(results.length).toEqual(TRANSACTION_COUNT);
        expect(callCount).toEqual(TRANSACTION_COUNT, 'Call count inaccurate');
        expect(transactionCount).toEqual(TRANSACTION_COUNT, 'Transaction count inaccurate');
        expect(assertCount).toEqual(TRANSACTION_COUNT, 'Assert count inaccurate');
        done();
      })
      .fail(function(err) {
        console.log('some transactions failed');
        done(err);
      })
      .done();
  });

  afterEach(function(done) {
    var self = this;
    self.sps
      .transaction()
      .query(function(client) {
        return Q
          .nfapply(fs.readFile, [
            __dirname + '/multi-transaction-teardown.sql',
            'utf8'
          ])
          .then(function(teardownSql) {
            return client.queryPromise(teardownSql);
          });
      })
      .promiseNoData()
      .then(function() {
        return self.sps.end();
      })
      .then(done)
      .fail(done)
      .done();
  });
});