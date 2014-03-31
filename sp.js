'use strict';

var fluent = require('fluentchain');
var fs = require('fs')
var pg = require('pg');
var Q = require('q');
var util = require('util');

var DEBUG_ENABLED = !!process.env['API_NG_DEBUG_ENABLED']


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

var NO_TRANSACTION = 1;
var TRANSACTION_ONGOING = 2;
var TRANSACTION_ENDED = 3;

var CONNECTED = 4;
var DISCONNECTED = 5;

function clientErrorHandling(err) {
  if(err.message.indexOf('terminating connection due to administrator command') !== -1) {
    //ignore
  } else {
    // TODO: re-emit as part of our connection?
    console.log('error in pg client: ' + err.message);
  }
}  

function Connection(client, done) {
  var self = this;
  var status = NO_TRANSACTION;
  var connection_status = CONNECTED;

  client.removeAllListeners('error', clientErrorHandling);
  client.on('error', clientErrorHandling);

  self.active = function() {
    return status === TRANSACTION_ONGOING;
  }

  self.connected = function() {
    return connection_status === CONNECTED;
  }

  self.begin = function() {
    if(status !== NO_TRANSACTION) {
      return Q.reject(new Error('Transaction already started or ended'));
    }

    var deferred = Q.defer();
    client.query('BEGIN', function(err) {
      if(err) {
        self.rollback().then(function() {
          deferred.reject(new Error('Could not begin transaction ' + err));
        });
      }
      status = TRANSACTION_ONGOING;
      deferred.resolve();
    });
    return deferred.promise;    
  }

  self.commit = function() {
    if(status !== TRANSACTION_ONGOING) {
      return Q.reject(new Error('Transaction already started or ended: ' + status));
    }    
    var deferred = Q.defer();
    client.query('COMMIT', function(err) {
      if(err) {
        self.rollback().then(function() {
          deferred.reject(new Error('Could not commit transaction ' + err));
        });
      }
      DEBUG_ENABLED && console.log('returning client to pool');
      done();
      status = TRANSACTION_ENDED;
      deferred.resolve();
    });
    return deferred.promise;    
  }

  self.rollback = function() {
    if(status !== TRANSACTION_ONGOING) {
      return Q.reject(new Error('Transaction not started or already ended'));
    }        
    var deferred = Q.defer();
    client.query('ROLLBACK', function(err) {      
      done(err);
      status = TRANSACTION_ENDED;      
      if(err) {
        deferred.reject(new Error('Could not rollback transaction ' + err));
      } else {
        deferred.resolve();  
      }
    });
    return deferred.promise;    
  }

  self.query = function(query, args) {
    if(status !== TRANSACTION_ONGOING) {
      return Q.reject(new Error('Transaction not started or already ended'));
    }          
    var deferred = Q.defer();
    
    client.query(query, args, function(err, results) {
      if(err) {
        self.rollback().then(function() {
          deferred.reject(new Error(query +' with [' + args + '] failed: ' + err.message));
        });
        return;
      }          
      deferred.resolve(results);
    });

    return deferred.promise;
  }

  self.queryNoTransaction = function(query, args) {
    if(status !== NO_TRANSACTION) {
      return Q.reject(new Error('Transaction exists'));
    }          
    var deferred = Q.defer();
    
    client.query(query, args, function(err, results) {
      if(err) {
        deferred.reject(new Error(query +' with [' + args + '] failed: ' + err.message));
        return;
      }          
      deferred.resolve(results);
    });

    return deferred.promise;
  }   
}

function connect(dsn) {
  DEBUG_ENABLED && console.log('connect called with ' + dsn);
  var deferred = Q.defer();

  pg.connect(dsn, function(err, client, done) {
    if(err) {
      deferred.reject(new Error('error fetching client from pool ' + err));
      return;
    }
    deferred.resolve(new Connection(client, done));
  });

  return deferred.promise;
}

function bindStoredProcToFluent(target, results){
  results.rows.forEach(function(row) {
    var params = generate_args_placeholder(row.argstypes.length);
    target[row.proname] = function() {
      var args = Array.prototype.slice.call(arguments); 
      target.chain(function() {
        var query = 'SELECT ' + row.proname + params;
        return target.connection
          .query(query, args)
          .then(function(results) {
            DEBUG_ENABLED && console.log('executed ' + row.proname);
            return results.rows[0][row.proname];
          });         
      })
      return target;
    }
  });  
}


function Transaction(dsn, results, role) {
  Transaction.super_.call(this);

  var self = this;

  // start with connecting (ie: getting client from client pool)
  // and then start transaction
  // and then set local user role (if specified)
  self.chain(function() {
      return connect(dsn)
        .then(function(conn) {
          DEBUG_ENABLED && console.log('connected to ' + dsn);
          self.connection = conn;
          return self.connection.begin();
        })
        .then(function() {
          if(role) {
            DEBUG_ENABLED && console.log('setting role ' + role);
            return self.connection.query('SET LOCAL ROLE ' + role);
          }
        });
    });


  // monkey-patch to autocommit transactions...
  self._promiseNoData = self.promiseNoData;
  self._promiseData = self.promiseData;

  self.promiseData = function() {
    self.chain(function(previousStepData) {
      if(self.connection.active()) {
        return self.connection
          .commit()
          .then(function() {
            return previousStepData;
          });  
      } else {
        return previousStepData;
      }
    });

    return self._promiseData();
  }

  self.promiseNoData = function() {
    self.chain(function() {
      if(self.connection.active()) {
        return self.connection
          .commit()       
      }
    });

    return self._promiseNoData();
  }

  self.commit = function() {
    self.chain(function(previousStepData) {
      return self.connection
        .commit()
        .then(function() {
          return previousStepData;
        });
    });
    return self;    
  }

  self.rollback = function() {
    self.chain(function(previousStepData) {
      return self.connection
        .rollback()
        .then(function() {
          return previousStepData;
        });
    });
    return self;
  }

  bindStoredProcToFluent(self, results);    
}
util.inherits(Transaction, fluent);


function StoredProcs(dsn, results) {
  var self = this;
  self.COUNT = results.rowCount;

  // new client with its own connection and local role
  self.transaction = function(role) {
    return new Transaction(dsn, results, role);
  }
}


function exportStoredProcs(dsn) {
  var connection;
  return connect(dsn)
    .then(function(conn) {
      connection = conn;
      var queryListOfProcs = fs.readFileSync(
        __dirname + '/sp.sql',
        'utf8');

      return connection.queryNoTransaction(queryListOfProcs);
    })
    .then(function(results) {
      connection.commit();
      return new StoredProcs(dsn, results);
    });      
}


exports.exportStoredProcs = exportStoredProcs;