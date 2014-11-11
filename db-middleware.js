var mysql = require('mysql'),
    Promise = require('bluebird');
require('mysql-additions').enableExperimentalFeatures();


function getMysqlConnection(key, pool) {
  
  return pool.getConnectionAsync()
  .then(function(connection) {
    //TODO store the connections by key
    //TODO what to do if connection aborded ??? we need to release some how ???
    //TODO This should be probably done at the appending to the request

    if (!connection) {
      throw new Error("no connection received");
    }
    //TODO this should not happen we need to test this at the assigning
    //     and release if connection is closed (request is finished)

    
    return {
      key: key,
      connection: connection
    };
  });
}

function setup(options) {

  var pools = [];

  for (var key in options.dbs) {
    if (options.dbs.hasOwnProperty(key)) {

      //TODO check the engine !!!!!
      pools.push({
        key: key,
        pool: mysql.createPool(options.dbs[key]),
        cb: getMysqlConnection
      });
    }
  }

  function getConnections() {
    var connections = [];

    pools.forEach(function(pool) {
      connections.push(pool.cb(pool.key, pool.pool));
    });

    return Promise.all(connections);
  }

  function releaseDbs() {
    if (this._dbs) {
      for (var key in this._dbs) {
        if (this._dbs.hasOwnProperty(key)) {
          this._dbs[key].release();
        }
      }
      this._dbs = undefined;
    }
  }

  function db(key) {
    return this._dbs[key];
  }

  return function(req, res, next) {
    var end = res.end;

    getConnections()
    .then(function(dbs) {
      req._dbs = {};

      //TODO check if request is alive and response was not send !!!!
      dbs.forEach(function(value) {
        req._dbs[value.key] = value.connection;
      });
      next();

    }, function(e) {
      next(new Error("connections could not be opened"));
    });

    req.db = db;

    res.on("close", function() {
      releaseDbs.call(req);
    });

    res.end = function(chunk, encoding) {
      releaseDbs.call(req);
      end.apply(res, arguments);
    };

  };
}


exports.setup = setup;
