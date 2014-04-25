'use strict';

var sp = require('../sp.js');
var fs = require('fs');

function main(spConfig, $log) {
  var sps;
  var store = {};

  spConfig.onConnection = function(connection) {
    var example1 = fs.readFileSync(
        __dirname + '/example1.sql',
        'utf8').toString();
    return connection.client.queryPromise(example1);
  }

  return sp
    .exportStoredProcs(spConfig)
    .then(function(s) {
      sps = s;
      return sps
        .transaction()
        .example1_test_function()
        .store('strtest', store)
        .promiseData();
    })
    .then(function() {
      return sps
        .transaction()
        .example1_modify('testrecord', 0)
        .promiseData()
    })
    .then(function() {
      return sps
        .transaction()
        .example1_retrieve('testrecord')
        .store('step1', store)
        .example1_modify('testrecord', 10)
        .example1_retrieve('testrecord')
        .store('step2', store)
        .example1_modify('testrecord', 20)
        .example1_retrieve('testrecord')
        .store('step3', store)
        .rollback()
        .promiseNoData()
    })
    .then(function() {
      return sps
        .transaction()
        .example1_retrieve('testrecord')
        .rollback()
        .promiseData()
    })
    .then(function() {
      return sps.end();
    })
    .then(function() {
      return store;
    });
}

exports.main = main;

// Usage (assuming you already have a DB with your username):
//
// node example1.js postgresql://$USER@localhost/$USER
//
// Output:
//
// { strtest: 'Hello world', step1: 0, step2: 10, step3: 20 }

if(require.main === module) {
  var dsn = process.argv[2];
  if(!dsn) {
    console.log('need to specify a datasource');
    console.log('example: node example1.js postgresql://$USER@localhost/$USER');
    return;
  }
  console.log('connecting to ' + dsn);
  main({dsn: dsn}, console)
    .then(function(output) {
      console.log(JSON.stringify(output));
    })
    .fail(function(err) {
      console.log(err.message);
      console.log(err.stack);
    })
    .done();

}
