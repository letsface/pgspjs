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

var connectionCount = 0;

function Connection(client, spConfig, done) {
  var self = this;
  self.client = client;
  self.connectionCount = connectionCount++;
  function id() {
    return 'C' + self.connectionCount + ' ';
  }

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
      return Q.reject(new Error(id() + 'Transaction already started or ended'));
    }

    var deferred = Q.defer();
    client.query('BEGIN', function(err) {
      if(err) {
        self.rollback().then(function() {
          deferred.reject(new Error(id() + 'Could not begin transaction ' + err));
        });
      }
      status = TRANSACTION_ONGOING;
      deferred.resolve();
    });
    return deferred.promise;    
  }

  self.commit = function() {
    if(status !== TRANSACTION_ONGOING) {
      return Q.reject(new Error(id() + 'Transaction already started or ended: ' + status));
    }    
    var deferred = Q.defer();
    client.query('COMMIT', function(err) {
      if(err) {
        self.rollback().then(function() {
          deferred.reject(new Error(id() + 'Could not commit transaction ' + err));
        });
      }
      DEBUG_ENABLED && console.log(id() + 'returning client to pool');
      done(err);
      status = TRANSACTION_ENDED;
      deferred.resolve();
    });
    return deferred.promise;    
  }

  self.rollback = function() {
    if(status !== TRANSACTION_ONGOING) {
      return Q.reject(new Error(id() + 'Transaction not started or already ended'));
    }        
    var deferred = Q.defer();
    client.query('ROLLBACK', function(err) { 
      status = TRANSACTION_ENDED;      
      if(err) {
        DEBUG_ENABLED && console.log(id() + 'returning client to pool with error ' + err.message);
        done(err);        
        deferred.reject(new Error(id() + 'Could not rollback transaction ' + err));
      } else {
        DEBUG_ENABLED && console.log(id() + 'returning client after rollback');
        done();
        deferred.resolve();  
      }
    });
    return deferred.promise;    
  }

  function onQuery(client, query, args) {
    if(typeof spConfig.onQuery === 'function') {
      return spConfig.onQuery(client, query, args);
    } else {
      return Q();
    }
  }


  self.queryInTransaction = function(query, args) {
    if(status !== TRANSACTION_ONGOING) {
      return Q.reject(new Error(id() + 'Transaction not started or already ended'));
    }              
    
    return onQuery(client, query, args)
      .then(function() {
        DEBUG_ENABLED && console.log(id() + 'executing actual query ' + query);
        return client
          .queryPromise(query, args)
          .fail(function(err) {
            DEBUG_ENABLED && console.log(id() + 'rolling back transaction ' + err.message);
            return self
              .rollback()
              .then(function() {
                throw new Error(query +' with [' + args + '] failed: ' + err.message);
              });            
          });
      });
  }

  self.queryNoTransaction = function(query, args) {
    if(status !== NO_TRANSACTION) {
      return Q.reject(new Error(id() + 'Transaction exists'));
    }

    return onQuery(client, query, args)
      .then(function() {
        return client.queryPromise(query, args);
      });
  }

  self.end = function() {
    done();
  }
}

function bindStoredProcToFluent(target, results){
  results.rows.forEach(function(row) {
    var params = generate_args_placeholder(row.argstypes.length);
    target[row.proname] = function() {
      var args = Array.prototype.slice.call(arguments);
      var query = 'SELECT ' + row.proname + params + ';';

      target.chain(function() {
        // evaluate any closure at this point, just before the query
        for(var i = 0; i<args.length; i++) {
          if(typeof args[i] === 'function') {
            args[i] = args[i]();
          }
        }
        
        return target.connection
          .queryInTransaction(query, args)
          .then(function(results) {
            DEBUG_ENABLED && console.log('executed ' + row.proname);
            return results.rows[0][row.proname];
          });         
      })
      return target;
    }
  });  
}


var transactionCount = 0;

function Transaction(spConfig, results, role, connect) {
  Transaction.super_.call(this);
  var self = this;

  self.transactionId = transactionCount++;
  function id() {
    return 'T' + self.transactionId + ' ';
  }
  // start with connecting (ie: getting client from client pool)
  // and then start transaction
  self.chain(function() {
      return connect()
        .then(function(conn) {
          DEBUG_ENABLED && console.log('connected to ' + spConfig.dsn);
          self.connection = conn;
          return self.connection.begin();
        })
        .then(function() {
          if(typeof spConfig.onTransaction === 'function') {
            var onTransaction = spConfig.onTransaction(role);
            DEBUG_ENABLED && console.log(id() + 'executing onTransaction');
            // we send initialization within the current transaction
            return self.connection.queryInTransaction(onTransaction);            
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
        return self.connection.commit();    
      }
    });

    return self._promiseNoData();
  }

  self.query = function(customQueryPromise) {
    self.chain(function() {
      return customQueryPromise(self.connection.client);
    });

    return self;
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


function StoredProcs(dsn, results, connect) {
  var self = this;

  self.COUNT = results.rowCount;
  var pleaseUseTransaction = function() {
    throw new Error('Please use transaction method');
  }

  results.rows.map(function(r) {
    self[r.proname] = pleaseUseTransaction;
  });

  self.list = function() {
    return results.rows;
  }

  // new client with its own connection and local role
  self.transaction = function(role) {
    return new Transaction(dsn, results, role, connect);
  }

  self.end = function() {
    DEBUG_ENABLED && console.log('calling pg.end()');
    pg.end();
  }
}


function wrapInTransaction(statements)  {
  return 'BEGIN;\n' + statements + '\nCOMMIT;'
}


var queryCount = 0;


function createQueryPromiseFunction(client) {
  function queryPromise(query, args) {
    var currentCount = queryCount++;

    function id() {
      return 'Q' + currentCount + ' ';
    } 

    var deferredQueryResults = Q.defer();
    client.query(query, args, function(err, results) {
      if(err) {
        DEBUG_ENABLED && console.log(id() + 'Error found, rejecting ' + query);
        deferredQueryResults.reject(new Error(id() + 'Query error: ' + query + ' : ' + err));
      } else {
        DEBUG_ENABLED && console.log(id() + 'Results found, resolving ' + query);
        deferredQueryResults.resolve(results);
      }
    });
    DEBUG_ENABLED && console.log(id() + 'returning promise to query ' + query + ' with args ' + JSON.stringify(args));
    return deferredQueryResults.promise;
  };

  return queryPromise;
}


function exportStoredProcs(spConfig) {
  var connection;
  spConfig.schema = spConfig.schema ? spConfig.schema : 'public';

  if(typeof spConfig.dsn !== 'string') {
    throw new Error('dsn property needs to be specified');
  }

  function connect() {
    DEBUG_ENABLED && console.log('connect called with ' + spConfig.dsn);
    var deferredConnection = Q.defer();

    pg.connect(spConfig.dsn, function(err, client, done) {
      if(err) {
        deferredConnection.reject(new Error('error fetching client from pool ' + err));
        return;
      }

      DEBUG_ENABLED && console.log('retrieved client from pool with count ' + client.__pool_count);

      // monkey-patch a nicer promise interface to client
      client.queryPromise = createQueryPromiseFunction(client);

      // catch emitted errors
      client.on('error', function(err) {
        console.log(err.message);
      });

      // client is from pool; if it's the first time out of the pool
      // call initialization function
      var onConnectionPromise;
      if(client.__pool_count === 1 && spConfig.onConnection) {
        var onConnection = wrapInTransaction(spConfig.onConnection());
        DEBUG_ENABLED && console.log('executing onConnection');
        onConnectionPromise = client.queryPromise(onConnection)
          .then(function() { DEBUG_ENABLED && console.log('onConnection completed')});
      } else {
        onConnectionPromise = Q();
        
      }

      onConnectionPromise
        .then(function() {
          deferredConnection.resolve(new Connection(client, spConfig, done));
        })
        .fail(function(err) {
          deferredConnection.reject(new Error('error initing connection ' + err))
        })
        .done();
    });
    return deferredConnection.promise;
  }

  return connect()
    .then(function(conn) {
      connection = conn;
    })
    .then(function() {
      if(typeof spConfig.beforeLoad === 'function') {
        return spConfig.beforeLoad(connection.client);
      }
    })
    .then(function() {
      var queryListOfProcs = fs.readFileSync(
        __dirname + '/sp.sql',
        'utf8');

      return connection.queryNoTransaction(queryListOfProcs, [spConfig.schema]);
    })
    .then(function(results) {
      DEBUG_ENABLED && console.log('committing connection used to retrieved stored procedures');
      // if you did not have any transaction but want to 
      // release the client, call connection.end()
      connection.end();
      return new StoredProcs(spConfig, results, connect);    
    });      
}


exports.exportStoredProcs = exportStoredProcs;