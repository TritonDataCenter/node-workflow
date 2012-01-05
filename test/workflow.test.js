// Copyright 2012 Pedro P. Candel <kusorbox@gmail.com>. All rights reserved.
var util = require('util'),
    test = require('tap').test,
    uuid = require('node-uuid'),
    Workflow = require('../lib/workflow');

var job = {
  timeout: 3,
  workflow_uuid: 'bdfa0821-5071-4682-b965-88293149a8d2',
  name: 'A workflow name',
  exec_after: '2012-01-03T12:54:05.788Z',
  params: {
    "a": "1",
    "b": "2"
  },
  uuid: 'fb4c202d-19ed-4ed9-afda-8255aa7f38ad',
  target: '/foo/bar',
  status: 'running',
  results: [],
  chain: [],
  onerror: []
};

var aWorkflow;

test('setup', function(t) {
  // body...
  aWorkflow = new Workflow(job);
  t.ok(aWorkflow, 'workflow ok');
  t.equal(aWorkflow.exec_after.toISOString(), job.exec_after);
  t.equal(aWorkflow.results.length, 0);
  t.end();
});

test('a task which succeeds on 1st retry', function(t) {
  var task = {
    "uuid": uuid(),
    "name": 'A name',
    "body": function(job, cb) {
      return cb(null);
    }.toString()
  }
  aWorkflow.runTask(task, function(err) {
    t.ifError(err, 'task error');
    t.equal(aWorkflow.results.length, 1);
    var res = aWorkflow.results[0];
    t.equal(res.error, '');
    t.equal(res.result, 'OK');
    t.end();
  });
  
});

test('a task which succeeds on 2nd retry', function(t) {
  var task = {
    "uuid": uuid(),
    "name": 'A name',
    "retry": 2,
    "body": function(job, cb) {
      if (!job.foo) {
        job.foo = true;
        return cb('Foo was not defined');
      }
      return cb(null);
    }.toString()
  }
  aWorkflow.runTask(task, function(err) {
    t.ifError(err, 'task error');
    t.equal(aWorkflow.results.length, 2);
    var res = aWorkflow.results[1];
    t.equal(res.error, '');
    t.equal(res.result, 'OK');
    t.ok(job.foo);
    t.end();
  });
});

test('a task which fails and succeeds "onerror"', function(t) {
  var task = {
    "uuid": uuid(),
    "name": 'A name',
    "retry": 2,
    "body": function(job, cb) {
      return cb('Task body error');
    }.toString(),
    "onerror": function(err, job, cb) {
      job.the_err = err;
      return cb(null);
    }.toString()
  }
  aWorkflow.runTask(task, function(err) {
    t.ifError(err, 'task error');
    t.equal(aWorkflow.results.length, 3);
    var res = aWorkflow.results[2];
    t.equal(res.error, '');
    t.equal(res.result, 'OK');
    t.equal(job.the_err, 'Task body error');
    t.end();
  });
});


test('a task which fails and has no "onerror"', function(t) {
  var task = {
    "uuid": uuid(),
    "name": 'A name',
    "body": function(job, cb) {
      return cb('Task body error');
    }.toString()
  }
  aWorkflow.runTask(task, function(err) {
    t.ok(err, 'task error');
    t.equal(aWorkflow.results.length, 4);
    var res = aWorkflow.results[3];
    t.equal(res.error, 'Task body error');
    t.equal(res.result, '');
    t.end();
  });
});

test('a task which fails and "onerror" fails too', function(t) {
  var task = {
    "uuid": uuid(),
    "name": 'A name',
    "body": function(job, cb) {
      return cb('Task body error');
    }.toString(),
    "onerror": function(err, job, cb) {
      return cb('OnError error');
    }.toString()
  }
  aWorkflow.runTask(task, function(err) {
    t.ok(err, 'task error');
    t.equal(aWorkflow.results.length, 5);
    var res = aWorkflow.results[4];
    t.equal(res.error, 'OnError error');
    t.equal(res.result, '');
    t.end();
  });
});

test('a task which fails after two retries and has no "onerror"', function(t) {
  var task = {
    "uuid": uuid(),
    "name": 'A name',
    "retry": 2,
    "body": function(job, cb) {
      if (!job.bar) {
        job.bar = true;
        return cb('Bar was not defined');
      } else if (!job.baz){
        job.baz = true;
        return cb('Baz was not defined');
      }
      // Should not be called
      return cb(null);
    }.toString()
  }
  aWorkflow.runTask(task, function(err) {
    t.ok(err, 'task error');
    t.equal(aWorkflow.results.length, 6);
    var res = aWorkflow.results[5];
    t.equal(res.error, 'Baz was not defined');
    t.equal(res.result, '');
    t.ok(job.bar);
    t.ok(job.baz);
    t.end();
  });
});

test('a task which time out and succeeds "onerror"', function(t) {
  var task = {
    "uuid": uuid(),
    "name": 'A name',
    "timeout": 2,
    "body": function(job, cb) {
      setTimeout(function() {
        // Should not be called:
        return cb('Error within timeout');
      }, 2050);
    }.toString(),
    "onerror": function(err, job, cb) {
      job.the_err = err;
      return cb(null);
    }.toString()
  }
  aWorkflow.runTask(task, function(err) {
    t.ifError(err, 'task error');
    t.equal(job.the_err, 'timeout error');
    t.equal(aWorkflow.results.length, 7);
    var res = aWorkflow.results[6];
    t.equal(res.error, '');
    t.equal(res.result, 'OK');
    t.end();
  });
});

test('a workflow which suceeds', function(t) {
  // body...
  t.end();
});



test('teardown', function(t) {
  // body...
  t.end();
});
