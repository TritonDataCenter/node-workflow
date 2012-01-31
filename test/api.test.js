// Copyright 2012 Pedro P. Candel <kusorbox@gmail.com>. All rights reserved.
var test = require('tap').test,
    uuid = require('node-uuid'),
    SOCKET = '/tmp/.' + uuid(),
    util = require('util'),
    restify = require('restify'),
    log4js = require('log4js'),
    API = require('../lib/api');


///--- Globals
var api, server, client, backend;

var PORT = process.env.UNIT_TEST_PORT || 12345;
var TEST_DB_NUM = 15;

var config = {
  backend: {
    module: '../lib/workflow-redis-backend',
    opts: {
      db: TEST_DB_NUM,
      port: 6379,
      host: '127.0.0.1'
    }
  }
};

var Backend = require(config.backend.module);

log4js.setGlobalLogLevel('TRACE');

//--- Tests

test('throws on missing opts', function(t) {
  t.throws(function() {
    return new API();
  }, new TypeError('opts (Object) required'));
  t.end();
});


test('throws on missing backend', function(t) {
  t.throws(function() {
    return new API(config);
  }, new TypeError('backend (Object) required'));
  t.end();
});


test('throws on missing opts.log4js', function(t) {
  backend = new Backend(config.backend.opts);
  t.ok(backend, 'backend ok');

  t.throws(function() {
    return new API(config, backend);
  }, new TypeError('opts.log4js (Object) required'));
  t.end();
});


test('throws on missing opts.api', function(t) {
  config.log4js = log4js;
  t.throws(function() {
    return new API(config, backend);
  }, new TypeError('opts.api (Object) required'));
  t.end();
});



// --- Yes, I know it's not the canonical way to proceed setting up the suite
// right after you've ran some tests before but, it's handy here:
test('setup', function(t) {
  config.api = {
    port: PORT
  };
  backend.init(function() {
    backend.client.flushdb(function(err, res) {
      t.ifError(err, 'flush db error');
      t.equal('OK', res, 'flush db ok');
    });
    backend.client.dbsize(function(err, res) {
      t.ifError(err, 'db size error');
      t.equal(0, res, 'db size ok');
    });
    api = new API(config, backend);
    t.ok(api, 'api ok');
    server = api.server;
    t.ok(server, 'server ok');
    server.listen(PORT, '127.0.0.1', function() {
      client = restify.createJsonClient({
        log4js: log4js,
        url: 'http://127.0.0.1:' + PORT,
        type: 'json',
        version: '*'
      });
      t.ok(client, 'client ok');
      t.end();
    });
  });
});


test('GET /workflows empty', function(t) {
  client.get('/workflows', function(err, req, res, obj) {
    t.ifError(err);
    t.equal(res.statusCode, 200);
    t.equivalent([], obj);
    t.end();
  });
});


test('POST /workflows', function(t) {
  client.post('/workflows', {
    name: 'A workflow',
    chain: [{
      name: 'A Task',
      timeout: 30,
      retry: 3,
      body: function(job, cb) {
        return cb(null);
      }.toString()
    }],
    timeout: 180,
    onerror: [{
      name: 'Another task',
      body: function(job, cb) {
        return cb(null);
      }.toString()
    }]
  }, function(err, req, res, obj) {
    t.ifError(err);
    t.ok(obj.uuid);
    t.ok(util.isArray(obj.chain));
    t.ok(util.isArray(obj.onerror));
    t.equal(res.headers.location, '/workflows/' + obj.uuid);
    t.end();
  });
});


test('teardown', function(t) {
  server.close(function() {
    backend.quit(function() {
      t.end();
    });
  });
});
