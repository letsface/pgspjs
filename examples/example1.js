'use strict';

var pgspjs = require('../sp.js');
var fs = require('fs');

function main(dataSourceName, $log) {
  var sps;
  return pgspjs
    .connect(dataSourceName)
    .then(function(connection) {
      var example1 = fs.readFileSync(
        __dirname + '/example1.sql',
        'utf8');    
      return connection.queryNoTransaction(example1);
    })
    .then(function() {
      return pgspjs.exportStoredProcs(dataSourceName);
    })
    .then(function(s) {
      sps = s;
      return sps
        .transaction()
        .example1_test_function()
        .promiseData();
    })
    .then(function(result) {
      $log.log(result);
    })
    .then(function() {
      var store = {};
      return sps
        .transaction()
        .example1_modify('testrecord', 0)
        .promiseData()
    })    
    .then(function() {
      var store = {};
      return sps
        .transaction()
        .example1_retrieve('testrecord')
        .store(store, 'step1')
        .example1_modify('testrecord', 10)
        .example1_retrieve('testrecord')
        .store(store, 'step2')
        .example1_modify('testrecord', 20)
        .example1_retrieve('testrecord')
        .store(store, 'step3')
        .rollback()
        .promiseNoData()
        .then(function() {
          $log.log(JSON.stringify(store));
        });
    })
    .then(function() {
      return sps
        .transaction()      
        .example1_retrieve('testrecord')
        .rollback()
        .promiseData()
    })
    .then(function(value) {
      $log.log(value);
    });
}

exports.main = main;

// Usage:
//
// createdb testdb
// node example1.js postgresql://$USER@localhost/testdb 
//
// Output: 
//
// Hello world
// {"step1":0,"step2":10,"step3":20}
// 0
if(require.main === module) {
  var dsn = process.argv[2];
  if(!dsn) {
    console.log('need to specify a datasource');
    console.log('example: node example1.js postgresql://$USER@localhost/testdb');
    return;
  }
  console.log('connecting to ' + dsn);
  main(dsn, console)
    .fail(function(err) {
      console.log(err.message);
      console.log(err.stack);
    })
    .finally(function() {
      process.exit();
    })
    .done();
}
