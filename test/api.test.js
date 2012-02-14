// Copyright 2012 Pedro P. Candel <kusorbox@gmail.com>. All rights reserved.
var test = require('tap').test,
    uuid = require('node-uuid'),
    SOCKET = '/tmp/.' + uuid(),
    util = require('util'),
    path = require('path'),
    Logger = require('bunyan'),
    restify = require('restify'),
    API = require('../lib/api');


///--- Globals
var api, server, client, backend, wf_uuid, job_uuid;

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

var log = new Logger({
  name: 'workflow-api',
  streams: [{
    level: 'info',
    stream: process.stdout
  }, {
    level: 'trace',
    path: path.resolve(__dirname, '../logs/test.api.log')
  }],
  serializers: {
    err: Logger.stdSerializers.err,
    req: Logger.stdSerializers.req,
    res: restify.bunyan.serializers.response
  }
});


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


test('throws on missing opts.logger', function(t) {
  backend = new Backend(config.backend.opts);
  t.ok(backend, 'backend ok');

  t.throws(function() {
    return new API(config, backend);
  }, new TypeError('opts.logger (Object) required'));
  t.end();
});


test('throws on missing opts.api', function(t) {
  config.logger = log;
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
        log: log,
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
    wf_uuid = obj.uuid;
    t.end();
  });
});


test('POST /workflows duplicated wf name', function(t) {
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
    t.ok(err);
    t.equal(err.statusCode, 409);
    t.equal(err.name, 'ConflictError');
    t.ok(obj.message);
    t.ok(obj.message.match(/Workflow\.name/g));
    t.end();
  });
});


test('POST /workflows task missing body', function(t) {
  client.post('/workflows', {
    name: 'A workflow',
    chain: [{
      name: 'A Task',
      timeout: 30,
      retry: 3
    }],
    timeout: 180,
    onerror: [{
      name: 'Another task',
      body: function(job, cb) {
        return cb(null);
      }.toString()
    }]
  }, function(err, req, res, obj) {
    t.ok(err);
    t.equal(err.statusCode, 409);
    t.equal(err.name, 'ConflictError');
    t.ok(obj.message);
    t.equal(obj.message, 'Task body is required');
    t.end();
  });
});


test('GET /workflows not empty', function(t) {
  client.get('/workflows', function(err, req, res, obj) {
    t.ifError(err);
    t.equal(res.statusCode, 200);
    t.equal(obj.length, 1);
    t.ok(obj[0].uuid);
    t.ok(util.isArray(obj[0].chain));
    t.ok(util.isArray(obj[0].onerror));
    t.end();
  });
});


test('GET /workflows/:uuid', function(t) {
  client.get(
    '/workflows/' + wf_uuid,
    function(err, req, res, obj) {
      t.ifError(err);
      t.ok(obj.uuid);
      t.ok(util.isArray(obj.chain));
      t.ok(util.isArray(obj.onerror));
      t.end();
    });
});


test('GET /workflows/:uuid 404', function(t) {
  var a_uuid = uuid();
  client.get(
    '/workflows/' + a_uuid,
    function(err, req, res, obj) {
      t.ok(err);
      t.equal(err.statusCode, 404);
      t.equal(err.name, 'RestError');
      t.equal(obj.code, 'ResourceNotFound');
      t.ok(obj.message);
      t.equal(
        obj.message,
        'Workflow ' + a_uuid + ' not found'
      );
      t.end();
    });
});


test('PUT /workflows/:uuid', function(t) {
  client.put('/workflows/' + wf_uuid, {
    name: 'A workflow',
    chain: [{
      name: 'A Task',
      timeout: 30,
      retry: 3,
      body: function(job, cb) {
        return cb(null);
      }.toString()
    },
    {
      name: 'One more Task',
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
    t.equal(obj.chain.length, 2);
    t.ok(util.isArray(obj.onerror));
    t.end();
  });
});


test('PUT /workflows/:uuid 404', function(t) {
  var a_uuid = uuid();
  client.put('/workflows/' + a_uuid, {
    name: 'A workflow',
    chain: [{
      name: 'A Task',
      timeout: 30,
      retry: 3,
      body: function(job, cb) {
        return cb(null);
      }.toString()
    },
    {
      name: 'One more Task',
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
    t.ok(err);
    t.equal(err.statusCode, 404);
    t.equal(err.name, 'RestError');
    t.equal(obj.code, 'ResourceNotFound');
    t.ok(obj.message);
    t.equal(
      obj.message,
      'Workflow ' + a_uuid + ' not found'
    );
    t.end();
  });
});


test('PUT /workflows/:uuid missing task body', function(t) {
  client.put('/workflows/' + wf_uuid, {
    name: 'A workflow',
    chain: [{
      name: 'A Task',
      timeout: 30,
      retry: 3,
      body: function(job, cb) {
        return cb(null);
      }.toString()
    },
    {
      name: 'One more Task',
      timeout: 30,
      retry: 3
    }],
    timeout: 180,
    onerror: [{
      name: 'Another task',
      body: function(job, cb) {
        return cb(null);
      }.toString()
    }]
  }, function(err, req, res, obj) {
    t.ok(err);
    t.equal(err.statusCode, 409);
    t.equal(err.name, 'ConflictError');
    t.ok(obj.message);
    t.equal(obj.message, 'Task body is required');
    t.end();
  });

});


test('GET /jobs empty', function(t) {

  t.test('without execution filter', function(t) {
    client.get('/jobs', function(err, req, res, obj) {
      t.ifError(err);
      t.equal(res.statusCode, 200);
      t.equivalent([], obj);
      t.end();
    });
  });

  t.test('with execution filter', function(t) {
    client.get('/jobs?execution=queued', function(err, req, res, obj) {
      t.ifError(err);
      t.equal(res.statusCode, 200);
      t.equivalent([], obj);
      t.end();
    });
  });

  t.test('with wrong execution filter', function(t) {
    client.get('/jobs?execution=sleepy', function(err, req, res, obj) {
      t.ok(err);
      t.equal(err.statusCode, 409);
      t.equal(err.name, 'ConflictError');
      t.ok(obj.message);
      t.ok(obj.message.match(/execution/gi));
      t.end();
    });
  });

  t.end();
});


test('POST /jobs', function(t) {
  var aJob = {
    target: '/foo/bar',
    foo: 'bar'
  };

  t.test('without worfklow uuid', function(t) {
    client.post('/jobs', aJob, function(err, req, res, obj) {
      t.ok(err);
      t.equal(err.statusCode, 409);
      t.equal(err.name, 'ConflictError');
      t.ok(obj.message);
      t.ok(obj.message.match(/opts\.workflow/gi));
      t.end();
    });
  });

  t.test('with unexisting workflow uuid', function(t) {
    aJob.workflow = uuid();
    client.post('/jobs', aJob, function(err, req, res, obj) {
      t.ok(err);
      t.equal(err.statusCode, 409);
      t.equal(err.name, 'ConflictError');
      t.ok(obj.message);
      t.ok(obj.message.match(/unexisting workflow/gi));
      t.end();
    });
  });

  t.test('job ok', function(t) {
    aJob.workflow = wf_uuid;
    client.post('/jobs', aJob, function(err, req, res, obj) {
      t.ifError(err);
      t.ok(obj);
      t.equal(obj.execution, 'queued');
      t.ok(obj.uuid);
      t.ok(util.isArray(obj.chain));
      t.ok(util.isArray(obj.chain_results));
      t.ok(util.isArray(obj.onerror));
      t.equal(obj.workflow_uuid, wf_uuid);
      t.equivalent(obj.params, {foo: 'bar'});
      t.equal(obj.target, '/foo/bar');
      t.equal(res.headers.location, '/jobs/' + obj.uuid);
      job_uuid = obj.uuid;
      t.end();
    });
  });

  t.test('with duplicated target and params', function(t) {
    client.post('/jobs', aJob, function(err, req, res, obj) {
      t.ok(err);
      t.equal(err.statusCode, 409);
      t.equal(err.name, 'ConflictError');
      t.ok(obj.message);
      t.equal(
        obj.message,
        'Another job with the same target and params is already queued'
      );
      t.end();
    });
  });

  t.end();
});


test('GET /jobs not empty', function(t) {

  t.test('without execution filter', function(t) {
    client.get('/jobs', function(err, req, res, obj) {
      t.ifError(err);
      t.equal(res.statusCode, 200);
      t.ok(obj.length);
      t.equal(obj.length, 1);
      t.equal(obj[0].uuid, job_uuid);
      t.end();
    });
  });

  t.test('with execution filter', function(t) {
    client.get('/jobs?execution=queued', function(err, req, res, obj) {
      t.ifError(err);
      t.equal(res.statusCode, 200);
      t.ok(obj.length);
      t.equal(obj.length, 1);
      t.equal(obj[0].uuid, job_uuid);
      t.end();
    });
  });

  t.end();
});


test('GET /jobs/:uuid', function(t) {

  t.test('job ok', function(t) {
    client.get('/jobs/' + job_uuid, function(err, req, res, obj) {
      t.ifError(err);
      t.equal(res.statusCode, 200);
      t.equal(obj.uuid, job_uuid);
      t.end();
    });
  });

  t.test('job not found', function(t) {
    client.get('/jobs/' + uuid(), function(err, req, res, obj) {
      t.ok(err);
      t.equal(err.statusCode, 404);
      t.equal(err.name, 'RestError');
      t.equal(obj.code, 'ResourceNotFound');
      t.ok(obj.message);
      t.ok(obj.message.match(/not found/gi));
      t.end();
    });
  });

  t.end();
});


test('POST /jobs/:uuid/info', function(t) {
  t.test('with unexisting job', function(t) {
    client.post('/jobs/' + uuid() + '/info', {
      '10%': 'Task completed first step'
    }, function(err, req, res, obj) {
      t.ok(err);
      t.equal(err.statusCode, 404);
      t.equal(err.name, 'RestError');
      t.equal(obj.code, 'ResourceNotFound');
      t.ok(obj.message);
      t.equal(obj.message, 'Job does not exist. Cannot Update.');
      t.end();
    });
  });

  t.test('with existing job', function(t) {
    client.post('/jobs/' + job_uuid + '/info', {
      '10%': 'Task completed first step'
    }, function(err, req, res, obj) {
      t.ifError(err);
      t.equal(res.statusCode, 200);
      t.end();
    });
  });

  t.end();
});


test('GET /jobs/:uuid/info', function(t) {
  t.test('with unexisting job uuid', function(t) {
    client.get('/jobs/' + uuid() + '/info', function(err, req, res, obj) {
      t.ok(err);
      t.equal(err.statusCode, 404);
      t.equal(err.name, 'RestError');
      t.equal(obj.code, 'ResourceNotFound');
      t.ok(obj.message);
      t.equal(obj.message, 'Job does not exist. Cannot get info.');
      t.end();
    });
  });

  t.test('with existing job', function(t) {
    client.get('/jobs/' + job_uuid + '/info', function(err, req, res, obj) {
      t.ifError(err);
      t.equal(res.statusCode, 200);
      t.equivalent([{ '10%': 'Task completed first step' }], obj);
      t.end();
    });
  });
  t.end();
});


test('POST /jobs/:uuid/cancel', function(t) {
  t.test('with unexisting job uuid', function(t) {
    client.post(
      '/jobs/' + uuid() + '/cancel',
      {},
      function(err, req, res, obj) {
        t.ok(err);
        t.equal(err.statusCode, 404);
        t.equal(err.name, 'RestError');
        t.equal(obj.code, 'ResourceNotFound');
        t.ok(obj.message);
        t.ok(obj.message.match(/not found/gi));
        t.end();
      });
  });
  t.test('with existing job', function(t) {
    client.post(
      '/jobs/' + job_uuid + '/cancel',
      {},
      function(err, req, res, obj) {
        t.ifError(err);
        t.equal(res.statusCode, 200);
        t.equal(obj.uuid, job_uuid);
        t.equal(obj.execution, 'canceled');
        t.end();
      });
  });
  t.end();
});


test('DELETE /workflows/:uuid', function(t) {
  client.del('/workflows/' + wf_uuid,
    function(err, req, res, obj) {
      t.ifError(err);
      t.equal(res.statusCode, 204);
      t.end();
    });
});


test('DELETE /workflows/:uuid 404', function(t) {
  var a_uuid = uuid();
  client.del('/workflows/' + a_uuid,
    function(err, req, res, obj) {
      t.ok(err);
      t.equal(err.statusCode, 404);
      t.equal(err.name, 'RestError');
      t.equal(obj.code, 'ResourceNotFound');
      t.ok(obj.message);
      t.equal(
        obj.message,
        'Workflow ' + a_uuid + ' not found'
      );
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
