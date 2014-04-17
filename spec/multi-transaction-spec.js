'use strict';

var sp = require('../sp.js');
var fs = require('fs');
var spConfig = require('./spconfig.js')
var Q = require('q');

describe(__filename, function() {
  // console.log(JSON.stringify(spconfig));
  spConfig.beforeLoad  = function(client) {
    return Q
      .nfapply(fs.readFile, [__dirname + '/multi-transaction.sql', 'utf8'])
      .then(function(setupSql) {
        return client.queryPromise(setupSql);
      });
  }

  beforeEach(function(done) {
    var self = this;

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
  })

  it('large number of transactions work to completion', function(done) {
    var self = this;
    var promises = [];
  
    var TRANSACTION_COUNT = 50;
    var assertCount = 0;

    for(var i=0; i<TRANSACTION_COUNT; i++) {
      var promise = Q()
        .then(function() {
          return self.sps
            .transaction()
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
          expect(store).toEqual({create: {}, query: {}, update: {}, remove: {}});
          return store;
        });

      promises.push(promise);
    }

    Q.allSettled(promises)
      .then(function(results) {
        expect(results.length).toEqual(TRANSACTION_COUNT);
        expect(assertCount).toEqual(TRANSACTION_COUNT);
        done();
      })
      .fail(function(errors) {
        console.log('some transactions failed');
        done(errors[0]);
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
              'utf8'])
          .then(function(teardownSql) {
            return client.queryPromise(teardownSql);
          });
      })
      .promiseNoData()
      .then(function() {
        self.sps.end();
      })
      .then(done)
      .fail(done)
      .done();
  });
});