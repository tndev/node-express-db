var mysql = require('mysql'),
  when = require('when');


function getMysqlConnection(key, pool) {
  var d = when.defer();
  pool.getConnection(function(err, connection) {
    //TODO store the connections by key
    //TODO what to do if connection aborded ??? we need to release some how ???
    //TODO This should be probably done at the appending to the request

    if (err) {
      d.reject(err);
    }
    if (!connection) {
      d.reject("no connection received");
    }
    //TODO this should not happen we need to test this at the assigning
    //     and release if connection is closed (request is finished)

    connection.config.queryFormat = function(query, values) {
      if (!values) {
        return query;
      }

      return query.replace(/\:(\w+)/g, function(txt, key) {
        if (Object.hasOwnProperty.call(values, key)) {
          return this.escape(values[key]);
        }

        return txt;
      }.bind(this));
    };

    connection.insert = function(table, values) {
      var query = "INSERT INTO `" + table + "` SET ";

      var list = [];
      for (var key in values) {
        if (Object.hasOwnProperty.call(values, key)) {
          list.push("`" + key + "`=:" + key);

        }
      }

      query += list.join(", ");

      return this.query(query, values);
    };


    connection.update = function(table, values, condition, params) {
      var query = "UPDATE `" + table + "` SET ";

      var tmpParams = {};
      var list = [];

      for (var key in values) {
        if (Object.hasOwnProperty.call(values, key)) {

          tmpParams["updateval_" + key] = values[key];
          list.push("`" + key + "`=:" + "updateval_" + key);

        }
      }

      query += list.join(", ");

      for (var key in params) {
        if (Object.hasOwnProperty.call(params, key)) {

          tmpParams[key] = params[key];
        }
      }

      query += " WHERE " + condition;

      return this.query(query, tmpParams);
    };
    d.resolve({
      key: key,
      connection: connection
    });
  });

  return d.promise;
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

    return when.all(connections);
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

    when(getConnections()).then(function(dbs) {
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
