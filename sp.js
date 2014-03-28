'use strict';

var pg = require('pg');
var fs = require('fs')
var Q = require('q');
var fluent = require('fluentchain');
var util = require('util');

function generate_args_placeholder(count) {
  var params = '('
  for(var i=1; i<=count; i++) {
    params += '$' + i;
    if (i!=count) {
      params += ',';
    }
  }
  params += ')';
  return params;
}

function StoredProcs(client, results, done) {
  StoredProcs.super_.call(this);
  var self = this;
  self.COUNT = results.rowCount;

  function simpleCall(statement) {
    return function() {
      self.chain(function(previousStepData) {
        var deferred = Q.defer();
        client.query(statement, [], function(err, results) {
          if(err) {
            deferred.reject();
          }
          // pass along previous data if any
          deferred.resolve(previousStepData);
        });
        return deferred.promise;      
      });
      return self;
    }
  }

  self.begin = simpleCall('BEGIN');
  self.commit = simpleCall('COMMIT');
  self.rollback = simpleCall('ROLLBACK');

  results.rows.forEach(function(row) {
    var params = generate_args_placeholder(row.argstypes.length);
    self[row.proname] = function() {
      var args = Array.prototype.slice.call(arguments); 
      self.chain(function() {
        // console.log('proc name: ' + row.proname);
        // console.log('proc arg names: ' + row.argsnames);
        // console.log('proc arg types: ' + row.argstypes.join(','));
        // console.log('proc rettype: ' + row.rettype);
        var deferred = Q.defer();
        var query = 'SELECT ' + row.proname + params;
        client.query(query, args, 
          function(err, results) {
            done();
            if(err) {
              console.log('auto-rollback of current transaction');
              client.query('ROLLBACK', [], function(err, results) { done()});
              return deferred.reject(new Error(row.proname + '(' + row.argstypes.join(',')  + ') with [' + args + '] failed: ' + err.message));
            }
            return deferred.resolve(results.rows[0][row.proname]);
          }
        );
        return deferred.promise;            
      })
      return self;
    }
  });  
}
util.inherits(StoredProcs, fluent);

function exportStoredProcs(dsn) {
  var deferred = Q.defer();

  pg.connect(dsn, function(err, client, done) {
    if(err) {
      throw new Error(err);
    }
    var queryListOfProcs = fs.readFileSync(__dirname + '/sp.sql','utf8');
    //console.log('executing\n"' + query + '"');

    client.query(queryListOfProcs, function(err, results) {
      done();
      if(err) {
        deferred.reject(err);
      }
      var sps = new StoredProcs(client, results, done);
      if(!sps) {
        deferred.reject(new Error('Could not instantiate StoredProcs'));
      } else {
        deferred.resolve(sps);
      }      
    });
  });
  return deferred.promise;
}

exports.exportStoredProcs = exportStoredProcs;
