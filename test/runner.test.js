var util = require('util'),
    path = require('path'),
    fs = require('fs'),
    test = require('tap').test,
    uuid = require('node-uuid'),
    WorkflowRunner = require('../lib/runner'),
    WorkflowRedisBackend = require('../lib/workflow-redis-backend'),
    Factory = require('../lib/index').Factory;

// A DB for testing, flushed before and right after we're done with tests
var TEST_DB_NUM = 15;

var backend, identifier, runner, factory;

var okTask = {
      name: 'OK Task',
      retry: 1,
      body: function(job, cb) {
        return cb(null);
      }
    },
    failTask = {
      retry: 1,
      name: 'Fail Task',
      body: function(job, cb) {
        return cb('Fail task error');
      }
    },
    okWf, failWf;

test('setup', function(t) {
  identifier = uuid();
  backend = new WorkflowRedisBackend({
    port: 6379,
    host: '127.0.0.1',
    db: TEST_DB_NUM
  });
  t.ok(backend, 'backend ok');
  backend.init(function() {
    backend.client.flushdb(function(err, res) {
      t.ifError(err, 'flush db error');
      t.equal('OK', res, 'flush db ok');
    });
    backend.client.dbsize(function(err, res) {
      t.ifError(err, 'db size error');
      t.equal(0, res, 'db size ok');
    });
    runner = new WorkflowRunner(backend, {
      identifier: identifier,
      forks: 2,
      run_interval: 0.1
    });
    t.ok(runner);
    factory = Factory(backend);
    t.ok(factory);

    // okWf:
    factory.workflow({
      name: 'OK wf',
      chain: [okTask],
      timeout: 60
    }, function(err, wf) {
      t.ifError(err, 'ok wf error');
      t.ok(wf, 'OK wf OK');
      okWf = wf;
      // failWf:
      factory.workflow({
        name: 'Fail wf',
        chain: [failTask],
        timeout: 60
      }, function(err, wf) {
        t.ifError(err, 'Fail wf error');
        t.ok(wf, 'Fail wf OK');
        failWf = wf;
        t.end();
      });
    });
  });
});


test('runner identifier', function(t) {
  var runner = new WorkflowRunner(backend),
      identifier;
  t.ifError(runner.identifier);
  // run getIdentifier twice, one to create the file,
  // another to just read it:
  runner.getIdentifier(function(err, id) {
    t.ifError(err);
    t.ok(id);
    identifier = id;
    runner.getIdentifier(function(err, id) {
      t.ifError(err);
      t.equal(id, identifier);
      t.end();
    });
  });
});


test('runner run job now', function(t) {
  var d = new Date();
  t.ok(runner.runNow({exec_after: d.toISOString()}));
  // Set to the future, so it shouldn't run:
  d.setTime(d.getTime() + 10000);
  t.ok(!runner.runNow({exec_after: d.toISOString()}));
  t.end();
});


test('run job', function(t) {
  var aJob;
  factory.job({
    workflow: okWf,
    exec_after: '2012-01-03T12:54:05.788Z'
  }, function(err, job) {
    t.ifError(err, 'job error');
    t.ok(job, 'run job ok');
    aJob = job;
    runner.run();
    setTimeout(function() {
      runner.quit(function() {
        backend.getJob(aJob.uuid, function(err, job) {
          t.ifError(err, 'run job get job error');
          t.equal(job.execution, 'succeeded', 'job execution');
          t.equal(job.chain_results[0].result, 'OK');
          t.end();
        });
      });
    }, 10000);
  });
});


test('run job which fails', function(t) {
  var aJob;
  factory.job({
    workflow: failWf,
    exec_after: '2012-01-03T12:54:05.788Z'
  }, function(err, job) {
    t.ifError(err, 'job error');
    t.ok(job, 'job ok');
    aJob = job;
    runner.run();
    setTimeout(function() {
      runner.quit(function() {
        backend.getJob(aJob.uuid, function(err, job) {
          t.ifError(err, 'get job error');
          t.equal(job.execution, 'failed', 'job execution');
          t.equal(job.chain_results[0].error, 'Fail task error');
          t.end();
        });
      });
    }, 10000);
  });
});


test('runner init', function(t) {
  runner.init(function(err) {
    t.ifError(err, 'runner init error');
    runner.backend.getRunners(function(err, runners) {
      t.ifError(err, 'get runners error');
      t.ok(runners[identifier], 'runner id ok');
      t.ok(new Date(runners[identifier]), 'runner timestamp ok');
      t.end();
    });
  });
});


test('teardown', function(t) {
  var cfg_file = path.resolve(__dirname, '../config/workflow-indentifier');
  backend.quit(function() {
    path.exists(cfg_file, function(exist) {
      if (exist) {
        fs.unlink(cfg_file, function(err) {
          t.ifError(err);
          t.end();
        });
      } else {
        t.end();
      }
    });
  });
});

