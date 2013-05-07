var  _ = require('underscore')
, r    = require('rethinkdb')
;


// ================================================================
// ================== Helpers =====================================
// ================================================================
//



// ================================================================
// ================== Configs =====================================
// ================================================================


var R = exports.Repogo = function () {};
var CONN = null;
var connect = R.connect = function (func, river) {

  if (CONN) { return func(CONN); }

  r.connect({host:'localhost', port: 28015}, function (err, conn) {
    if (err) {
      if (river)
        river.finish('rethink-conn-error', err);
      else
        throw err;
      return;
    }

    CONN = conn;
    if (river)
      func(CONN, river);
    else
      func(CONN);
    return;
  });

}; // === function connect

R.close = function (f) {
  var result = CONN.close();
  return (f) ? f(result) : result;
};

R.rethinkdb = function (func) {
  connect(function (conn) {
    func(conn, r);
  });
};

R.tables = function (db_name, func) {
  R.rethinkdb(function (conn, r) {
    r.db(db_name).tableList().run(conn, function (err, tables) {
      if (err) throw err;
      func(tables, conn, r.db(db_name), r);
    });
  });
};

// ================================================================
// ================== Main Stuff ==================================
// ================================================================

R.new = function (db, table) {
  var i   = new R;
  i.db    = db;
  i.table = table;
  return i;
};

// ================================================================
// ================== CREATE ======================================
// ================================================================

R.prototype.create = function (data, flow) {
};




// ================================================================
// ================== READ ========================================
// ================================================================


R.prototype.read_by_id = function (id, flow) {
  return this.read_one({id: id}, flow);
};

R.prototype.read_one = function (doc, flow) {
  var me   = this;
  var vals = [];
  var sql  = "SELECT * FROM @table WHERE @vals LIMIT 1 ;"
  .replace('@table', me.quoted_table)
  .replace('@vals', doc_to_and(doc, vals));

  T.run(me, sql, vals, flow.reply(function (j, last) {
    j.finish(last[0]);
  }));
};

R.prototype.read_list = function (doc, flow) {
  var me      = this;
  var row     = null;
  var started = false;
  _.each(doc, function (val, key) {
    if (started) {
      row = row.and(r.row(key).eq(val));
    } else {
      row = r.row(key).eq(val);
    }
  });
  connect(function (conn) {
    r.db(me.db)
    .table(me.table)
    .filter(row)
    .run(conn, function (err, res) {
      if (err) {
        if (flow) {
          flow.finish('rethinkdb-query-error', err);
        } else {
          throw err;
        }
      } else {
        var arr = [];
        res.each(function (err, row) {
          arr.push(row);
        });
        flow.finish(arr);
      }
    });
  });
};




// ================================================================
// ================== UPDATE ======================================
// ================================================================

R.prototype.update = function (where, set, flow) {

  if (_.isString(where) || _.isNumber(where))
    where = {id: where};

  var me = this;
  var sql_and_vals = doc_to_update_sql(set, where);
  var vals         = sql_and_vals.vals;
  var sql          = sql_and_vals.sql.replace('@table', me.quoted_table);

  T.run(me, sql, vals, flow.reply(function (j) {
    if (where.id)
      flow.finish(j.result[0]);
    else
      flow.finish(j.result);
  }));
};

R.prototype.update_and_stamp = function (where, set, flow) {
  set['updated_at'] = '$now';
  return this.update(where, set, flow);
};

// ================================================================
// ================== Trash/Untrash================================
// ================================================================

R.prototype.untrash = function (id, j) {
  var me = this;
  me.update(id, {trashed_at: null}, j);
};

R.prototype.trash = function (id, flow) {
  var me = this;
  me.run("UPDATE " + me.quoted_table +  " SET trashed_at = $now WHERE id = $1 RETURNING id, trashed_at;", [id], flow.reply(function (j, last) {
    j.finish(last[0]);
  }));
};

// ================================================================
// ================== DELETE ======================================
// ================================================================

R.prototype.delete_trashed = function (days, flow) {
  if (arguments.length < 2) {
    flow = arguments[0];
    days = 2
  }
  var time = T.days_ago(days);

  var sql = "\
    DELETE FROM " + this.quoted_table + "         \
    WHERE trashed_at IS NOT NULL AND              \
          trashed_at < $1    \
    RETURNING *;        \
  ";

  T.run(this, sql, [ time ], flow);
  return this;
};


















