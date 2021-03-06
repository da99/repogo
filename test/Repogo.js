
var _      = require('underscore')
, assert   = require('assert')
, River    = require('da_river').River
, Repogo   = require('../lib/Repogo').Repogo
;

var TABLE = "REPOGO_TEST";
before(function (done) {

  Repogo.tables('test', function (tables, conn, db, r) {

    if (tables.indexOf(TABLE) === -1) {

      db.tableCreate(TABLE, {primaryKey: 'id'}).run(conn, function (err, result) {
        if (err) throw err;
        console.log(result);
        done();
      });

    } else {

      db.table(TABLE).delete().run(conn, function (err, result) {
        if (err) throw err;
        console.log(result);
        done();
      });

    }
  });

});

after(function () {
  console.log('closing')
  Repogo.close();
});


it( 'abc', function (done) {
  Repogo.rethinkdb(function (conn, r) {
    r.db('test').table(TABLE).insert({ superhero: 'Iron Man', superpower: 'Arc Reactor' })
    .run(conn, function (err, r) {
      done();
    });
  });
});

it( 'abc', function (done) {
  Repogo.rethinkdb(function (conn, r) {
    r.db('test').table(TABLE).insert({ superhero: 'Iron Man', superpower: 'Arc Reactor' })
    .run(conn, function (err, r) {
      done();
    });
  });
});

it( 'abc', function (done) {
  Repogo.rethinkdb(function (conn, r) {
    r.db('test').table(TABLE).insert({ superhero: 'Iron Man', superpower: 'Arc Reactor' })
    .run(conn, function (err, r) {
      done();
    });
  });
});

it( 'abc', function (done) {
  Repogo.rethinkdb(function (conn, r) {
    r.db('test').table(TABLE).insert({ superhero: 'Iron Man', superpower: 'Arc Reactor' })
    .run(conn, function (err, r) {
      done();
    });
  });
});

it( 'abc', function (done) {
  Repogo.rethinkdb(function (conn, r) {
    r.db('test').table(TABLE).insert({ superhero: 'Iron Man', superpower: 'Arc Reactor' })
    .run(conn, function (err, r) {
      done();
    });
  });
});

it( 'abc', function (done) {
  Repogo.rethinkdb(function (conn, r) {
    r.db('test').table(TABLE).insert({ superhero: 'Iron Man', superpower: 'Arc Reactor' })
    .run(conn, function (err, r) {
      console.log(arguments);
      done();
    });
  });
});

describe( 'Repogo', function () {

  // before(function (done) {
    // R(done)
    // .job(function (j) {
      // Repogo.run("DROP TABLE IF EXISTS \"" + table + "\";", [], j);
    // })
    // .job(function (j) {
      // Repogo.run("CREATE TABLE IF NOT EXISTS \"" + table +
                 // "\" (\n" +
                 // " id serial PRIMARY KEY, \n" +
                 // " name varchar(10), \n" +
                 // " body text ,   \n" +
                 // " $created_at , \n" +
                 // " $updated_at , \n" +
                 // " $trashed_at   \n);", [], j);
    // })
    // .run();
  // });

  // before(function (done) {
    // R(done)
    // .job(function (j) {
      // var sql = 'INSERT INTO \"' + table +  '\" (name, body) VALUES ($1, $2) RETURNING * ;';
      // Repogo.run(sql, [name, body], j);
    // })
    // .job(function (j, last) {
      // id = last[0].id;
      // j.finish();
    // })
    // .run();
  // });

  // after(function (done) {
    // R(done)
    // .job(function (j) {
      // Repogo.run("DELETE FROM \"" + table + "\";", [], j);
    // })
    // .run();
  // });

  describe( '.run', function () {
    it( 'uses process.DATABASE_URL by default', function (done) {
      River.new(null)
      .job(function (j) {
        Repogo.run("SELECT now()", {}, j);
      })
      .job(function (j, result) {
        assert.equal(H.is_date(result[0].now), true);
        done();
      })
      .run();
    });
  }); // === end desc

  // ================================================================
  // ================== CREATE ======================================
  // ================================================================


  describe( '.create', function () {
    it( 'inserts object as row', function (done) {
      var body = Math.random(1000) + "";
      River.new(null)
      .job(function (j) {
        Repogo.new(table).create({name: "hi 1", body: body}, j);
      })
      .job(function (j, result) {
        assert.equal(result.id > 0, true);
        assert.equal(result.body, body);
        done();
      })
      .run();
    });
  }); // === end desc


  // ================================================================
  // ================== READ ========================================
  // ================================================================

  describe( '.read', function () {

    describe( '.read_by_id', function () {

      it( 'returns a single result', function (done) {
        River.new(null)
        .job(function (j) {
          T.read_by_id(id, j);
        })
        .job(function (j, result) {
          assert.equal(result.id, id);
          assert.equal(result.body, body);
          done();
        })
        .run();
      });

    }); // === end desc

    describe( '.read_one', function () {

      it( 'returns a single result', function (done) {
        River.new(null)
        .job(function (j) {
          T.read_one({body: body}, j);
        })
        .job(function (j, last) {
          assert.equal(last.id, id);
          done();
        })
        .run();
      });
    }); // === end desc

    describe( '.read_list', function () {

      it( 'returns a list', function (done) {
        River.new(null)
        .job(function (j) {
          T.read_list({body: body}, j);
        })
        .job(function (j, last) {
          assert.equal(last.length, 1);
          done();
        })
        .run();
      });

    }); // === end desc

  }); // === end desc



  // ================================================================
  // ================== UPDATE ======================================
  // ================================================================


  describe( '.update', function () {

    it( 'updates record with string id', function (done) {
      body = "new body " + rand();
      River.new(null)
      .job(function (j) {
        T.update(id.toString(), {body: body}, j);
      })
      .job(function (j, last) {
        assert.equal(last.id, id);
        Q.query('SELECT * from \"' + table + '\" WHERE body = $1 LIMIT 1;', [body], function (err, result) {
          if (err) throw err;
          var row = result.rows[0];
          assert.equal(row.body, body);
          assert.equal(row.id, id);
          done();
        });
      }).run();
    });

  }); // === end desc

  describe( '.update_and_stamp', function () {
    it( 'updates record with time stamp', function (done) {
      body = "new body " + rand();
      River.new(null)
      .job(function (j) {
        T.update_and_stamp(id.toString(), {body: body}, j);
      })
      .job(function (j, last) {
        assert.equal(last.id, id);
        Q.query('SELECT * from \"' + table + '\" WHERE body = $1 LIMIT 1;', [body], function (err, result) {
          if (err) throw err;
          var row = result.rows[0];
          assert.equal(row.body, body);
          assert.equal(row.id, id);
          assert.equal(is_recent(row.updated_at), true);
          done();
        });
      }).run();
    });
  }); // === end desc

  // ================================================================
  // ================== Trash/Untrash ===============================
  // ================================================================

  describe( '.trash', function () {
    it( 'updates column trashed_at to: timestamp epoch', function (done) {

      var l = ((new Date).getTime() + '').length;

      River.new(null)
      .job(function (j) {
        T.trash(id, j);
      })
      .job(function (j, last) {
        assert.equal(is_recent(last.trashed_at), true);
        j.finish();
      })
      .job(function (j) {
        T.read_by_id(id, j);
      })
      .job(function (j, last) {
        assert.equal(is_recent(last.trashed_at), true);
        done();
      })
      .run();
    });

    it( 'passes first record to next job', function (done) {
      River.new(null)
      .job(function (j) {
        T.trash(id, j);
      })
      .job(function (j, last) {
        assert.equal(last.id, id);
        done();
      })
      .run();
    });
  }); // === end desc

  describe( '.untrash', function () {
    it( 'updates column trashed_at to: null', function (done) {
      River.new(null)
      .job(function (j) {
        T.trash(id, j);
      })
      .job(function (j) {
        T.untrash(id, j);
      })
      .job(function (j, last) {
        T.read_by_id(id, j);
      })
      .job(function (j, last) {
        assert.equal(last.trashed_at, null);
        assert.equal(last.id, id);
        done();
      })
      .run();
    });

    it( 'passes first record to next job', function (done) {
      River.new(null)
      .job(function (j) {
        T.untrash(id, j);
      })
      .job(function (j, last) {
        assert.equal(last.id, id);
        done();
      })
      .run();
    });
  }); // === end desc

  describe( '.delete_trashed', function () {

    it( 'does not delete records younger than days specified', function (done) {
      var day_4 = H.days_ago(4);
      var day_almost_4 = H.days_ago(4, 3000);

      River.new(null)
      .job(function (j) {
        T.update(id, {trashed_at: day_almost_4}, j);
      })
      .job(function (j, last) {
        T.delete_trashed(4, j);
      })
      .job(function (j, last) {
        assert.equal(last.length, 0);
        j.finish();
      })
      .job(function (j) {
        T.read_by_id(id, j);
      })
      .job(function (j, last) {
        assert.equal(last.id, id);
        done();
      })
      .run();
    });

    it( 'deletes records older than days specified', function (done) {
      var day_3 = H.days_ago(3);
      River.new(null)
      .job(function (j) {
        T.update(id, {trashed_at: day_3}, j);
      })
      .job(function (j, last) {
        T.delete_trashed(3, j);
      })
      .job(function (j, last) {
        assert.equal(last[0].id, id);
        done();
      })
      .run();
    });


  }); // === end desc
}); // === end desc







