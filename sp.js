'use strict';

var fluent = require('fluentchain');
var fs = require('fs')
var pg = require('pg');
var Q = require('q');
var util = require('util');


var DEBUG_ENABLED = !!process.env['API_NG_DEBUG_ENABLED']
Q.longStackSupport = DEBUG_ENABLED;

pg.on('error', function(err) {
  console.log('PG emitted error: ' + err.message);
});

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

function Connection(client, spConfig, done, context) {
  var self = this;
  self.client = client;
  self.connectionCount = connectionCount++;
  function id() {
    return 'C' + self.connectionCount + ':' + context;
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

    return client
      .queryPromise('BEGIN')
      .then(function() {
        status = TRANSACTION_ONGOING;
      })
      .fail(function(err) {
        status = TRANSACTION_ENDED;
        var msg = id() + 'Could not begin transaction ' + err;
        console.log(msg);
        done(err);
      });
  }

  self.commit = function() {
    if(status !== TRANSACTION_ONGOING) {
      return Q.reject(new Error(id() + 'Transaction not started or ended: ' + status));
    }
    return client
      .queryPromise('COMMIT')
      .then(function() {
        DEBUG_ENABLED && console.log(id() + 'returning client to pool');
        done();
        status = TRANSACTION_ENDED;
      })
      .fail(function(err) {
        self.rollback().done(
          function() { done(err); },
          function() {
            var msg = id() + 'Could not commit transaction ' + err;
            console.log(msg);
            // whether rollback fails or not, we return client with err
            done(err);
          });
      });
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
                console.log(query +' with [' + args + '] failed: ' + err.message);
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

function Transaction(spConfig, results, role, connect, context) {
  Transaction.super_.call(this);

  var self = this;

  context = context ? context + ' ' : process.pid +' ';

  self.transactionId = transactionCount++;
  function id() {
    return 'T' + self.transactionId + ':' + role + ':' + context;
  }
  // start with connecting (ie: getting client from client pool)
  // and then start transaction
  self.chain(function() {
      return connect(context)
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

function pgEndPromise() {
  DEBUG_ENABLED && console.log('calling pg.end()');

  var deferred = Q.defer();
  pg.once('end', function() {
    DEBUG_ENABLED && console.log('pg.end received');
    deferred.resolve();
  });
  pg.once('error', function(err) {
    deferred.reject(err);
  });

  var pool = pg.pools.all[Object.keys(pg.pools.all)[0]];
  if(pool) {
    var inUse = pool.getPoolSize() - pool.availableObjectsCount();
    if(inUse) {
      console.log('WARNING: You have called pg.end() but you still have connections in use: ' + inUse);
    }
  }
  pg.end();
  return deferred.promise;
}

function StoredProcs(spConfig, results, connect) {
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
  self.transaction = function(role, context) {
    return new Transaction(spConfig, results, role, connect, context);
  }

  self.end = function() {
    var beforeEnd;

    if(typeof spConfig.beforeEnd == 'function') {
      DEBUG_ENABLED && console.log('executing beforeEnd');
      beforeEnd = connect('beforeEnd')
        .then(function(connection) {
          return connection.begin()
            .then(function() {
              return spConfig.beforeEnd(connection);
            })
            .then(function() {
              return connection.commit();
            })
            .then(function() {
              DEBUG_ENABLED && console.log('beforeEnd completed');
            });
        });
    } else {
      beforeEnd = Q();
    }

    return beforeEnd
      .then(function() {
        return pgEndPromise();
      });
  }
}

var queryCount = 0;

function createQueryPromiseFunction(client, context) {
  function queryPromise(query, args) {
    var currentCount = queryCount++;

    function id() {
      return 'Q' + currentCount + ': ' + context + ' ';
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
  spConfig.schema = spConfig.schema ? spConfig.schema : 'public';

  if(typeof spConfig.dsn !== 'string') {
    throw new Error('dsn property needs to be specified');
  }

  function connect(context) {
    DEBUG_ENABLED && console.log('connect called with ' + spConfig.dsn);
    var deferredConnection = Q.defer();

    pg.connect(spConfig.dsn, function(err, client, done) {
      if(err) {
        deferredConnection.reject(new Error('error fetching client from pool ' + err));
        return;
      }

      DEBUG_ENABLED && console.log('retrieved client from pool with count ' + client.poolCount);

      // monkey-patch a nicer promise interface to client
      client.queryPromise = createQueryPromiseFunction(client, context);

      // catch emitted errors
      client.on('error', function(err) {
        console.log(err.message);
      });

      // client is from pool; if it's the first time out of the pool
      // call initialization function
      var connection = new Connection(client, spConfig, done, context);
      var onConnectionPromise;
      if(client.poolCount === 1 && spConfig.onConnection) {
        DEBUG_ENABLED && console.log('executing onConnection');
        onConnectionPromise = spConfig
          .onConnection(connection)
          .then(function() {
            DEBUG_ENABLED && console.log('onConnection completed')
          });
      } else {
        onConnectionPromise = Q();
      }

      onConnectionPromise
        .then(function() {
          deferredConnection.resolve(connection);
        })
        .fail(function(err) {
          deferredConnection.reject(new Error('error initing connection ' + err))
        })
        .done();
    });
    return deferredConnection.promise;
  }

  return connect('exportStoredProcs')
    .then(function(connection) {
      return connection
      .begin()
      .then(function() {
        if(typeof spConfig.beforeLoad === 'function') {
          DEBUG_ENABLED && console.log('calling beforeLoad');
          return spConfig.beforeLoad(connection);
        }
      })
      .then(function() {
        var queryListOfProcs = fs.readFileSync(
          __dirname + '/sp.sql',
          'utf8');

        return connection.queryInTransaction(queryListOfProcs, [spConfig.schema]);
      })
      .then(function(results) {
        DEBUG_ENABLED && console.log('committing connection used to retrieved stored procedures');
        // if you did not have any transaction but want to
        // release the client, call connection.end()
        return connection
          .commit()
          .then(function() {
           return new StoredProcs(spConfig, results, connect);
          });
      })
    });
}


exports.exportStoredProcs = exportStoredProcs;