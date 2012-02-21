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

var logDir = path.resolve(__dirname, '../logs');

var exists = path.existsSync(logDir);
if (!exists) {
  fs.mkdirSync(logDir, '0777');
}

var config = {};

var okTask = {
  name: 'OK Task',
  retry: 1,
  body: function (job, cb) {
    return cb(null);
  }
},
failTask = {
  retry: 1,
  name: 'Fail Task',
  body: function (job, cb) {
    return cb('Fail task error');
  }
},
okWf, failWf, theJob;


test('throws on missing opts', function (t) {
  t.throws(function () {
    return new WorkflowRunner();
  }, new TypeError('opts (Object) required'));
  t.end();
});


test('throws on missing backend', function (t) {
  t.throws(function () {
    return new WorkflowRunner(config);
  }, new TypeError('backend (Object) required'));
  t.end();
});

test('setup', function (t) {
  identifier = uuid();
  config.runner = {
    identifier: identifier,
    forks: 2,
    run_interval: 0.1
  };
  backend = new WorkflowRedisBackend({
    port: 6379,
    host: '127.0.0.1',
    db: TEST_DB_NUM
  });
  t.ok(backend, 'backend ok');
  backend.init(function () {
    backend.client.flushdb(function (err, res) {
      t.ifError(err, 'flush db error');
      t.equal('OK', res, 'flush db ok');
    });
    backend.client.dbsize(function (err, res) {
      t.ifError(err, 'db size error');
      t.equal(0, res, 'db size ok');
    });
    runner = new WorkflowRunner(config, backend);
    t.ok(runner);
    factory = Factory(backend);
    t.ok(factory);

    // okWf:
    factory.workflow({
      name: 'OK wf',
      chain: [okTask],
      timeout: 60
    }, function (err, wf) {
      t.ifError(err, 'ok wf error');
      t.ok(wf, 'OK wf OK');
      okWf = wf;
      // failWf:
      factory.workflow({
        name: 'Fail wf',
        chain: [failTask],
        timeout: 60
      }, function (err, wf) {
        t.ifError(err, 'Fail wf error');
        t.ok(wf, 'Fail wf OK');
        failWf = wf;
        t.end();
      });
    });
  });
});


test('runner identifier', function (t) {
  var runner = new WorkflowRunner({runner: {}}, backend),
      identifier;
  t.ifError(runner.identifier);
  // run getIdentifier twice, one to create the file,
  // another to just read it:
  runner.getIdentifier(function (err, id) {
    t.ifError(err);
    t.ok(id);
    identifier = id;
    runner.getIdentifier(function (err, id) {
      t.ifError(err);
      t.equal(id, identifier);
      t.end();
    });
  });
});


test('runner run job now', function (t) {
  var d = new Date();
  t.ok(runner.runNow({exec_after: d.toISOString()}));
  // Set to the future, so it shouldn't run:
  d.setTime(d.getTime() + 10000);
  t.ok(!runner.runNow({exec_after: d.toISOString()}));
  t.end();
});


test('idle runner', function (t) {
  factory.job({
    workflow: okWf.uuid,
    exec_after: '2012-01-03T12:54:05.788Z'
  }, function (err, job) {
    t.ifError(err, 'job error');
    t.ok(job, 'run job ok');
    theJob = job;
    // The job is queued. Now we'll idle the runner and verify it will not
    // touch the job
    backend.idleRunner(runner.identifier, function (err) {
      t.ifError(err, 'idle runner error');
      runner.run();
      setTimeout(function () {
        runner.quit(function () {
          backend.getJob(theJob.uuid, function (err, job) {
            t.ifError(err, 'run job get job error');
            t.equal(job.execution, 'queued', 'job execution');
            t.end();
          });
        });
      }, 7000);
    });
  });
});


test('run job', function (t) {
  // Let's remove the idleness of the runner so it will pick the job
  backend.wakeUpRunner(runner.identifier, function (err) {
    t.ifError(err, 'wake up runner error');
    runner.run();
    setTimeout(function () {
      runner.quit(function () {
        backend.getJob(theJob.uuid, function (err, job) {
          t.ifError(err, 'run job get job error');
          t.equal(job.execution, 'succeeded', 'job execution');
          t.equal(job.chain_results[0].result, 'OK');
          t.end();
        });
      });
    }, 7000);
  });
});


test('run job which fails', function (t) {
  var aJob;
  factory.job({
    workflow: failWf.uuid,
    exec_after: '2012-01-03T12:54:05.788Z'
  }, function (err, job) {
    t.ifError(err, 'job error');
    t.ok(job, 'job ok');
    aJob = job;
    runner.run();
    setTimeout(function () {
      runner.quit(function () {
        backend.getJob(aJob.uuid, function (err, job) {
          t.ifError(err, 'get job error');
          t.equal(job.execution, 'failed', 'job execution');
          t.equal(job.chain_results[0].error, 'Fail task error');
          t.end();
        });
      });
    }, 9000);
  });
});


test('runner init', function (t) {
  runner.init(function (err) {
    t.ifError(err, 'runner init error');
    runner.backend.getRunners(function (err, runners) {
      t.ifError(err, 'get runners error');
      t.ok(runners[identifier], 'runner id ok');
      t.ok(new Date(runners[identifier]), 'runner timestamp ok');
      t.end();
    });
  });
});


test('inactive runners', function (t) {
  // Add another runner, which we'll set as inactive
  var theUUID = uuid(),
  anotherRunner = new WorkflowRunner({runner: {
    identifier: theUUID,
    forks: 2,
    run_interval: 0.1
  }}, backend);
  t.ok(anotherRunner, 'another runner ok');
  // Init the new runner, then update it to make inactive
  anotherRunner.init(function (err) {
    t.ifError(err, 'another runner init error');
    // Now we quit the new runner, and outdate it:
    anotherRunner.quit(function () {
      runner.inactiveRunners(function (err, runners) {
        t.ifError(err, 'inactive runners error');
        t.ok(util.isArray(runners), 'runners is array');
        t.equal(runners.length, 0, 'runners length');
        backend.runnerActive(
          anotherRunner.identifier,
          '2012-01-03T12:54:05.788Z',
          function (err) {
            t.ifError(err, 'set runner timestamp error');
            runner.inactiveRunners(function (err, runners) {
              t.ifError(err, 'inactive runners error');
              t.ok(util.isArray(runners), 'runners is array');
              t.equal(runners.length, 1, 'runners length');
              t.equal(runners[0], theUUID, 'runner uuid error');
              t.end();
            });
          });
      });
    });
  });
});


test('stale jobs', function (t) {
  // Add another runner, which we'll set as inactive
  var theUUID = uuid(),
  anotherRunner = new WorkflowRunner({runner: {
    identifier: theUUID,
    forks: 2,
    run_interval: 0.1
  }}, backend),
  aJob;
  t.ok(anotherRunner, 'another runner ok');

  factory.job({
    workflow: okWf.uuid,
    exec_after: '2012-01-03T12:54:05.788Z'
  }, function (err, job) {
    t.ifError(err, 'job error');
    t.ok(job, 'run job ok');
    aJob = job;
    // Init the new runner, then update it to make inactive
    anotherRunner.init(function (err) {
      t.ifError(err, 'another runner init error');
      backend.runJob(aJob.uuid, anotherRunner.identifier, function (err, job) {
        t.ifError(err, 'backend run job error');
        // The runner is not inactive; therefore, no stale jobs
        runner.staleJobs(function (err, jobs) {
          t.ifError(err, 'stale jobs error');
          t.equivalent(jobs, [], 'stale jobs empty');
          // Now we quit the new runner, and outdate it:
          anotherRunner.quit(function () {
            // The runner will be inactive so, any job flagged as owned by
            // this runner will be stale
            backend.runnerActive(
              anotherRunner.identifier,
              '2012-01-03T12:54:05.788Z',
              function (err) {
                t.ifError(err, 'set runner timestamp error');
                // Our job should be here now:
                runner.staleJobs(function (err, jobs) {
                  t.ifError(err, 'stale jobs 2 error');
                  t.equivalent([aJob.uuid], jobs);
                  // Let's set the job canceled and finish it:
                  backend.updateJobProperty(
                    aJob.uuid,
                    'execution',
                    'canceled',
                    function (err) {
                      t.ifError(err, 'update job prop err');
                      aJob.execution = 'canceled';
                      backend.finishJob(job, function (err, job) {
                        t.ifError(err, 'finish job err');
                        t.ok(job, 'finish job ok');
                        runner.staleJobs(function (err, jobs) {
                          t.ifError(err, 'stale jobs 3 error');
                          t.equivalent(jobs, []);
                          t.end();
                        });
                      });
                    });
                });
              });
          });
        });
      });
    });

  });
});

test('teardown', function (t) {
  var cfg_file = path.resolve(__dirname, '../config/workflow-indentifier');
  backend.quit(function () {
    path.exists(cfg_file, function (exist) {
      if (exist) {
        fs.unlink(cfg_file, function (err) {
          t.ifError(err);
          t.end();
        });
      } else {
        t.end();
      }
    });
  });
});
