var pg = require('pg').native;
var express = require('express');
var morgan = require('morgan');
var bodyParser = require('body-parser');

var connString = 'postgres://eventstore@localhost/eventstore';
var app = express();

pg.defaults.poolSize = 50;

app.use(morgan('short'));
app.use(bodyParser.json());

app.post('/events/:id/:type', function(req, res) {
  var event = req.body;
  event.id = req.params.id;
  event.type = req.params.type;
  event.received_at = new Date().toISOString();

  pg.connect(connString, function(err, client, done) {
    client.query(
      `insert into events
      (id, type, created_at, received_at, payload)
      values ($1, $2, $3, $4, $5)
      on conflict (id) do nothing;`,
      [event.id, event.type, event.data.meta.created_at, event.received_at, event],
      function(err, result) {
        done();

        if (err) {
          res.status(500).json(err).end();
        } else {
          if (result.rowCount === 0) {
            res.status(200);
          } else {
            res.status(201);
          }

          res.end();
        }
      }
    );
  });
});

function streamEventsOut(res, done) {
  return function(err, result) {
    done();

    if (err) {
      res.status(500).json(err).end();
    }

    res.append('Content-Type', 'application/json');
    res.write('[');

    for (var i = 0; i < result.rowCount; i++) {
      res.write(result.rows[i].payload);

      if (i < result.rowCount - 1) {
        res.write(',');
      }
    }

    res.write(']');
    res.end();
  };
}

app.get('/events/:from/?', function(req, res) {
  var from = req.params.from;

  pg.connect(connString, function(err, client, done) {
    var query = client.query(
      `select payload::text
      from events
      where received_at >= $1
      order by received_at
      limit 500;`,
      [from],
      streamEventsOut(res, done)
    );
  });
});

app.get('/events/:from/:types', function(req, res) {
  var from = req.params.from;
  var types = req.params.types.split(',');

  pg.connect(connString, function(err, client, done) {
    var query = client.query(
      `select payload::text
      from events
      where received_at >= $1
      and type = any($2::text[])
      order by received_at
      limit 500;`,
      [from, types],
      streamEventsOut(res, done)
    );
  });
});

app.listen(3000);
