// Copyright 2012 Pedro P. Candel <kusorbox@gmail.com>. All rights reserved.
var test = require('tap').test,
    uuid = require('node-uuid'),
    SOCKET = '/tmp/.' + uuid(),
    util = require('util'),
    path = require('path'),
    fs = require('fs'),
    Logger = require('bunyan'),
    restify = require('restify'),
    API = require('../lib/api');


// --- Globals
var api, server, client, backend, wf_uuid, job_uuid;

var config = {};

config.logger = {
    streams: [ {
        level: 'info',
        stream: process.stdout
    }, {
        level: 'trace',
        path: path.resolve(__dirname, './test.api.log')
    }]
};

var helper = require('./helper');

var REQ_ID = uuid();
// --- Tests

test('throws on missing opts', function (t) {
    t.throws(function () {
        return API();
    }, new TypeError('opts (Object) required'));
    t.end();
});


test('throws on missing backend', function (t) {
    t.throws(function () {
        return API(config);
    }, new TypeError('opts.backend (Object) required'));
    t.end();
});


test('throws on missing opts.api', function (t) {
    config.backend = helper.config().backend;

    t.throws(function () {
        return API(config);
    }, new TypeError('opts.api (Object) required'));
    t.end();
});



// --- Yes, I know it's not the canonical way to proceed setting up the suite
// right after you've ran some tests before but, it's handy here:
test('setup', function (t) {
    config.api = helper.config().api;
    api = API(config);
    t.ok(api, 'api ok');
    server = api.server;
    t.ok(server, 'server ok');
    backend = api.backend;
    t.ok(backend, 'backend ok');
    api.init(function () {
        client = restify.createJsonClient({
            log: api.log,
            url: 'http://127.0.0.1:' + helper.config().api.port,
            version: '*',
            retryOptions: {
                retry: 0
            }
        });
        t.ok(client, 'client ok');
        t.end();
    });
});


test('GET /workflows empty', function (t) {
    client.get('/workflows', function (err, req, res, obj) {
        t.ifError(err);
        t.equal(res.statusCode, 200);
        t.equivalent([], obj);
        t.end();
    });
});


test('POST /workflows', function (t) {
    client.post('/workflows', {
        name: 'A workflow',
        chain: [ {
            name: 'A Task',
            timeout: 30,
            retry: 3,
            body: function (job, cb) {
                return cb(null);
            }.toString()
        }],
        timeout: 180,
        onerror: [ {
            name: 'Another task',
            body: function (job, cb) {
                return cb(null);
            }.toString()
        }]
    }, function (err, req, res, obj) {
        t.ifError(err, 'POST /workflows error');
        t.ok(obj.uuid, 'Workflow UUID ok');
        t.ok(Array.isArray(obj.chain), 'Workflow chain is an Array');
        t.ok(Array.isArray(obj.onerror), 'Workflow onerror is an Array');
        t.equal(res.headers.location, '/workflows/' + obj.uuid, 'Location ok');
        t.equal(obj.uuid, res.headers['x-request-id'], 'x-request-id ok');
        wf_uuid = obj.uuid;
        t.end();
    });
});


test('POST /workflows duplicated wf name', function (t) {
    client.headers['x-request-id'] = REQ_ID;
    client.post('/workflows', {
        name: 'A workflow',
        chain: [ {
            name: 'A Task',
            timeout: 30,
            retry: 3,
            body: function (job, cb) {
                return cb(null);
            }.toString()
        }],
        timeout: 180,
        onerror: [ {
            name: 'Another task',
            body: function (job, cb) {
                return cb(null);
            }.toString()
        }]
    }, function (err, req, res, obj) {
        t.ok(err);
        t.equal(err.statusCode, 409);
        t.equal(err.restCode, 'InvalidArgument');
        t.ok(err.message.match(/Workflow\.name/g));
        t.equal(REQ_ID, res.headers['x-request-id']);
        t.end();
    });
});


test('POST /workflows task missing body', function (t) {
    client.post('/workflows', {
        name: 'A workflow',
        chain: [ {
            name: 'A Task',
            timeout: 30,
            retry: 3
        }],
        timeout: 180,
        onerror: [ {
            name: 'Another task',
            body: function (job, cb) {
                return cb(null);
            }.toString()
        }]
    }, function (err, req, res, obj) {
        t.ok(err);
        t.equal(err.statusCode, 409, 'error status code');
        t.equal(err.name, 'ConflictError', 'error name');
        t.equal(err.body.message, 'Task body is required', 'error body');
        t.end();
    });
});


test('GET /workflows not empty', function (t) {
    client.get('/workflows', function (err, req, res, obj) {
        t.ifError(err);
        t.equal(res.statusCode, 200);
        t.equal(obj.length, 1);
        t.ok(obj[0].uuid);
        t.ok(util.isArray(obj[0].chain));
        t.ok(util.isArray(obj[0].onerror));
        t.end();
    });
});


test('GET /workflows/:uuid', function (t) {
    client.get(
        '/workflows/' + wf_uuid,
        function (err, req, res, obj) {
            t.ifError(err);
            t.ok(obj.uuid);
            t.ok(util.isArray(obj.chain));
            t.ok(util.isArray(obj.onerror));
            t.end();
        });
});


test('GET /workflows/:uuid 404', function (t) {
    var a_uuid = uuid();
    client.get(
        '/workflows/' + a_uuid,
        function (err, req, res, obj) {
            t.ok(err);
            t.equal(err.statusCode, 404);
            t.equal(err.restCode, 'ResourceNotFound');
            t.ok(err.message.match(/does not exist/g));
            t.end();
        });
});


test('PUT /workflows/:uuid', function (t) {
    client.put('/workflows/' + wf_uuid, {
        name: 'A workflow',
        chain: [ {
            name: 'A Task',
            timeout: 30,
            retry: 3,
            body: function (job, cb) {
                return cb(null);
            }.toString()
        },
        {
            name: 'One more Task',
            timeout: 30,
            retry: 3,
            body: function (job, cb) {
                return cb(null);
            }.toString()
        }],
        timeout: 180,
        onerror: [ {
            name: 'Another task',
            body: function (job, cb) {
                return cb(null);
            }.toString()
        }]
    }, function (err, req, res, obj) {
        t.ifError(err);
        t.ok(obj.uuid);
        t.ok(util.isArray(obj.chain));
        t.equal(obj.chain.length, 2);
        t.ok(util.isArray(obj.onerror));
        t.end();
    });
});


test('PUT /workflows/:uuid 404', function (t) {
    var a_uuid = uuid();
    client.put('/workflows/' + a_uuid, {
        name: 'A workflow',
        chain: [ {
            name: 'A Task',
            timeout: 30,
            retry: 3,
            body: function (job, cb) {
                return cb(null);
            }.toString()
        },
        {
            name: 'One more Task',
            timeout: 30,
            retry: 3,
            body: function (job, cb) {
                return cb(null);
            }.toString()
        }],
        timeout: 180,
        onerror: [ {
            name: 'Another task',
            body: function (job, cb) {
                return cb(null);
            }.toString()
        }]
    }, function (err, req, res, obj) {
        t.ok(err);
        t.equal(err.statusCode, 404);
        t.equal(err.restCode, 'ResourceNotFound');
        t.ok(err.message);
        t.end();
    });
});


test('PUT /workflows/:uuid missing task body', function (t) {
    client.put('/workflows/' + wf_uuid, {
        name: 'A workflow',
        chain: [ {
            name: 'A Task',
            timeout: 30,
            retry: 3,
            body: function (job, cb) {
                return cb(null);
            }.toString()
        },
        {
            name: 'One more Task',
            timeout: 30,
            retry: 3
        }],
        timeout: 180,
        onerror: [ {
            name: 'Another task',
            body: function (job, cb) {
                return cb(null);
            }.toString()
        }]
    }, function (err, req, res, obj) {
        t.ok(err);
        t.equal(err.statusCode, 409);
        t.equal(err.name, 'ConflictError');
        t.equal(err.body.message, 'Task body is required');
        t.end();
    });

});


test('GET /jobs empty', function (t) {

    t.test('without execution filter', function (t) {
        client.get('/jobs', function (err, req, res, obj) {
            t.ifError(err);
            t.equal(res.statusCode, 200);
            t.equivalent([], obj);
            t.end();
        });
    });

    t.test('with execution filter', function (t) {
        client.get('/jobs?execution=queued', function (err, req, res, obj) {
            t.ifError(err);
            t.equal(res.statusCode, 200);
            t.equivalent([], obj);
            t.end();
        });
    });

    t.test('with wrong execution filter', function (t) {
        client.get('/jobs?execution=sleepy', function (err, req, res, obj) {
            t.ok(err);
            t.equal(err.statusCode, 409);
            t.equal(err.name, 'ConflictError');
            t.ok(err.message.match(/execution/gi));
            t.end();
        });
    });

    t.end();
});


test('POST /jobs', function (t) {
    var aJob = {
        target: '/foo/bar',
        foo: 'bar',
        chicken: 'arise!'
    };

    t.test('without worfklow uuid', function (t) {
        client.post('/jobs', aJob, function (err, req, res, obj) {
            t.ok(err);
            t.equal(err.statusCode, 409);
            t.equal(err.name, 'ConflictError');
            t.ok(err.message.match(/j\.workflow/gi));
            t.end();
        });
    });

    t.test('with unexisting workflow uuid', function (t) {
        aJob.workflow = uuid();
        client.post('/jobs', aJob, function (err, req, res, obj) {
            t.ok(err);
            t.equal(err.statusCode, 404);
            t.equal(err.restCode, 'ResourceNotFound');
            t.ok(err.message);
            t.end();
        });
    });

    t.test('job ok', function (t) {
        delete client.headers['x-request-id'];
        aJob.workflow = wf_uuid;
        client.post('/jobs', aJob, function (err, req, res, obj) {
            t.ifError(err);
            t.ok(obj);
            t.equal(obj.execution, 'queued');
            t.ok(obj.uuid);
            t.ok(util.isArray(obj.chain));
            t.ok(util.isArray(obj.chain_results));
            t.ok(util.isArray(obj.onerror));
            t.equal(obj.workflow_uuid, wf_uuid);
            t.equivalent(obj.params, {foo: 'bar', chicken: 'arise!'});
            t.equal(obj.target, '/foo/bar');
            t.equal(res.headers.location, '/jobs/' + obj.uuid);
            t.equal(obj.uuid, res.headers['x-request-id'], 'x-request-id ok');
            job_uuid = obj.uuid;
            t.end();
        });
    });

    t.test('with duplicated target and params', function (t) {
        client.post('/jobs', aJob, function (err, req, res, obj) {
            t.ok(err);
            t.equal(err.statusCode, 409);
            t.equal(err.restCode, 'InvalidArgument');
            t.equal(
              err.message,
              'Another job with the same target and params is already queued');
            t.end();
        });
    });


    t.test('with duplicated target and different params', function (t) {
        aJob.chicken = 'egg';
        client.post('/jobs', aJob, function (err, req, res, obj) {
            t.ifError(err);
            t.ok(obj);
            t.equivalent(obj.params, {foo: 'bar', chicken: 'egg'});
            t.end();
        });
    });


    t.end();
});


test('GET /jobs not empty', function (t) {

    t.test('without execution filter', function (t) {
        client.get('/jobs', function (err, req, res, obj) {
            t.ifError(err, 'get jobs error');
            t.equal(res.statusCode, 200, 'get jobs status code');
            t.ok(obj.length, 'obj is an array');
            t.equal(obj.length, 2, 'total jobs found');
            t.end();
        });
    });

    t.test('with execution filter', function (t) {
        client.get('/jobs?execution=queued', function (err, req, res, obj) {
            t.ifError(err, 'get jobs error');
            t.equal(res.statusCode, 200, 'get jobs status code');
            t.ok(obj.length, 'obj is an array');
            t.equal(obj.length, 2, 'total jobs found');
            t.end();
        });
    });


    t.test('with execution and params filter', function (t) {
        client.get('/jobs?execution=queued&foo=bar',
          function (err, req, res, obj) {
            t.ifError(err, 'get jobs error');
            t.equal(res.statusCode, 200, 'get jobs status code');
            t.ok(obj.length, 'obj is an array');
            t.equal(obj.length, 2, 'total jobs found');
            t.end();
        });
    });


    t.test('with params filter', function (t) {
        client.get('/jobs?chicken=egg', function (err, req, res, obj) {
            t.ifError(err, 'get jobs error');
            t.equal(res.statusCode, 200, 'get jobs status code');
            t.ok(obj.length, 'obj is an array');
            t.equal(obj.length, 1, 'total jobs found');
            t.end();
        });
    });

    t.end();
});


test('GET /jobs/:uuid', function (t) {

    t.test('job ok', function (t) {
        client.get('/jobs/' + job_uuid, function (err, req, res, obj) {
            t.ifError(err);
            t.equal(res.statusCode, 200);
            t.equal(obj.uuid, job_uuid);
            t.end();
        });
    });

    t.test('job not found', function (t) {
        client.get('/jobs/' + uuid(), function (err, req, res, obj) {
            t.ok(err);
            t.equal(err.statusCode, 404);
            t.equal(err.restCode, 'ResourceNotFound');
            t.ok(obj.message);
            t.end();
        });
    });

    t.end();
});


test('POST /jobs/:uuid/info', function (t) {
    t.test('with unexisting job', function (t) {
        client.post('/jobs/' + uuid() + '/info', {
            '10%': 'Task completed first step'
        }, function (err, req, res, obj) {
            t.ok(err);
            t.equal(err.statusCode, 404);
            t.equal(err.restCode, 'ResourceNotFound');
            t.ok(err.message);
            t.equal(obj.message, 'Job does not exist. Cannot Update.');
            t.end();
        });
    });

    t.test('with existing job', function (t) {
        client.post('/jobs/' + job_uuid + '/info', {
            '10%': 'Task completed first step'
        }, function (err, req, res, obj) {
            t.ifError(err);
            t.equal(res.statusCode, 200);
            t.end();
        });
    });

    t.end();
});


test('GET /jobs/:uuid/info', function (t) {
    t.test('with unexisting job uuid', function (t) {
        client.get('/jobs/' + uuid() + '/info', function (err, req, res, obj) {
            t.ok(err);
            t.equal(err.statusCode, 404);
            t.equal(err.restCode, 'ResourceNotFound');
            t.ok(err.message);
            t.equal(err.message, 'Job does not exist. Cannot get info.');
            t.end();
        });
    });

    t.test('with existing job', function (t) {
        client.get('/jobs/' + job_uuid + '/info',
          function (err, req, res, obj) {
            t.ifError(err);
            t.equal(res.statusCode, 200);
            t.equivalent([ { '10%': 'Task completed first step' }], obj);
            t.end();
        });
    });
    t.end();
});


test('POST /jobs/:uuid/cancel', function (t) {
    t.test('with unexisting job uuid', function (t) {
        client.post(
          '/jobs/' + uuid() + '/cancel',
          {},
          function (err, req, res, obj) {
            t.ok(err);
            t.equal(err.statusCode, 404);
            t.equal(err.restCode, 'ResourceNotFound');
            t.ok(err.message);
            t.end();
          });
    });
    t.test('with existing job', function (t) {
        client.post(
          '/jobs/' + job_uuid + '/cancel',
          {},
          function (err, req, res, obj) {
            t.ifError(err);
            t.equal(res.statusCode, 200);
            t.equal(obj.uuid, job_uuid);
            t.equal(obj.execution, 'canceled');
            t.end();
          });
    });
    t.end();
});


test('DELETE /workflows/:uuid', function (t) {
    client.del('/workflows/' + wf_uuid,
        function (err, req, res, obj) {
            t.ifError(err);
            t.equal(res.statusCode, 204);
            t.end();
        });
});


test('DELETE /workflows/:uuid 404', function (t) {
    var a_uuid = uuid();
    client.del('/workflows/' + a_uuid,
        function (err, req, res, obj) {
            t.ok(err);
            t.equal(err.statusCode, 404);
            t.equal(err.restCode, 'ResourceNotFound');
            t.ok(err.message);
            t.end();
        });
});


test('teardown', function (t) {
    server.close(function () {
        backend.quit(function () {
            t.end();
        });
    });
});
