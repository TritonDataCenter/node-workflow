// Copyright 2012 Pedro P. Candel <kusorbox@gmail.com>. All rights reserved.
var util = require('util'),
    test = require('tap').test,
    uuid = require('node-uuid'),
    WorkflowJobRunner = require('../lib/job-runner'),
    Logger = require('bunyan'),
    Factory = require('../lib/index').Factory;

var helper = require('./helper');

var logger = {
  name: 'workflow-runner',
  serializers: {
    err: Logger.stdSerializers.err
  },
  streams: [ {
    level: 'trace',
    stream: process.stdout
  }]
};

var Backend = require(helper.config().backend.module),
    backend = new Backend(helper.config().backend.opts),
    factory, wf_job_runner;

var okWf, failWf, timeoutWf, reQueueWf, reQueuedJob, elapsed;

var FakeRunner = function () {
  this.child_processes = {};
  this.uuid = uuid();
  this.run_interval = 1000;
  this.log = new Logger(logger);
  this.slots = this.forks = 10;
};

FakeRunner.prototype.childUp = function (job_uuid, child_pid) {
  var self = this;
  self.child_processes[child_pid] = job_uuid;
};

FakeRunner.prototype.childDown = function (job_uuid, child_pid) {
  var self = this;
  // For real, we also want to send sigterm to the child process on job's
  // termination, therefore here we may need to upgrade on DB too.
  delete self.child_processes[child_pid];
};

FakeRunner.prototype.getSlot = function () {
  var self = this;
  if (self.slots === 0) {
    return false;
  } else {
    self.slots -= 1;
    return true;
  }
};

FakeRunner.prototype.releaseSlot = function () {
  var self = this;
  if (self.slots >= self.forks) {
    return false;
  } else {
    self.slots += 1;
    return true;
  }
};

var runner = new FakeRunner();

test('setup', function (t) {
  t.ok(backend, 'backend ok');
  backend.init(function () {
    factory = Factory(backend);
    t.ok(factory, 'factory ok');

    // okWf:
    factory.workflow({
      name: 'OK wf',
      chain: [ {
        name: 'OK Task',
        retry: 1,
        body: function (job, cb) {
          return cb(null);
        }
      }],
      timeout: 60
    }, function (err, wf) {
      t.ifError(err, 'ok wf error');
      t.ok(wf, 'OK wf OK');
      okWf = wf;
      // failWf:
      factory.workflow({
        name: 'Fail wf',
        chain: [ {
          retry: 1,
          name: 'Fail Task',
          body: function (job, cb) {
            return cb('Fail task error');
          }
        }],
        timeout: 60
      }, function (err, wf) {
        t.ifError(err, 'Fail wf error');
        t.ok(wf, 'Fail wf OK');
        failWf = wf;
        factory.workflow({
          name: 'Timeout Wf',
          chain: [ {
            name: 'Timeout Task',
            body: function (job, cb) {
              setTimeout(function () {
                // Should not be called:
                return cb('Error within timeout');
              }, 3050);
            }
          }],
          timeout: 3
        }, function (err, wf) {
          t.ifError(err, 'Timeout wf error');
          t.ok(wf, 'Timeout wf ok');
          timeoutWf = wf;
          factory.workflow({
            name: 'Re-Queue wf',
            chain: [ {
              name: 'OK Task',
              retry: 1,
              body: function (job, cb) {
                return cb(null);
              }
            }, {
              name: 'Re-Queue Task',
              body: function (job, cb) {
                return cb('queue');
              }
            }, {
              name: 'OK Task 2',
              retry: 1,
              body: function (job, cb) {
                return cb(null);
              }
            }],
            timeout: 60
          }, function (err, wf) {
            t.ifError(err, 'ReQueue wf error');
            t.ok(wf, 'ReQueue wf ok');
            reQueueWf = wf;
            t.end();
          });
        });
      });
    });
  });
});


test('throws on missing opts', function (t) {
  t.throws(function () {
    return new WorkflowJobRunner();
  }, new TypeError('opts (Object) required'));
  t.end();
});


test('throws on missing opts.runner', function (t) {
  t.throws(function () {
    return new WorkflowJobRunner({});
  }, new TypeError('opts.runner (Object) required'));
  t.end();
});


test('throws on missing opts.backend', function (t) {
  t.throws(function () {
    return new WorkflowJobRunner({
      runner: runner
    });
  }, new TypeError('opts.backend (Object) required'));
  t.end();
});



test('throws on missing opts.job', function (t) {
  t.throws(function () {
    return new WorkflowJobRunner({
      runner: runner,
      backend: backend
    });
  }, new TypeError('opts.job (Object) required'));
  t.end();
});


test('throws on incorrect opts.sandbox', function (t) {
  t.throws(function () {
    return new WorkflowJobRunner({
      runner: runner,
      backend: backend,
      job: {},
      sandbox: 'foo'
    });
  }, new TypeError('opts.sandbox must be an Object'));
  t.end();
});


test('run job ok', function (t) {
  factory.job({
    workflow: okWf.uuid,
    exec_after: '2012-01-03T12:54:05.788Z'
  }, function (err, job) {
    t.ifError(err, 'job error');
    t.ok(job, 'run job ok');
    wf_job_runner = new WorkflowJobRunner({
      runner: runner,
      backend: backend,
      job: job,
      trace: false
    });
    t.ok(wf_job_runner, 'wf_job_runner ok');
    backend.runJob(job.uuid, runner.uuid, function (err, job) {
      t.ifError(err, 'backend.runJob error');
      wf_job_runner.run(function (err) {
        t.ifError(err, 'wf_job_runner run error');
        backend.getJob(job.uuid, function (err, job) {
          t.ifError(err, 'backend.getJob error');
          t.equal(job.execution, 'succeeded');
          t.equal(job.chain_results.length, 1);
          t.equal(job.chain_results[0].result, 'OK');
          t.end();
        });
      });

    });
  });
});


test('run a job which fails without "onerror"', function (t) {
  factory.job({
    workflow: failWf.uuid,
    exec_after: '2012-01-03T12:54:05.788Z'
  }, function (err, job) {
    t.ifError(err, 'job error');
    t.ok(job, 'job ok');
    wf_job_runner = new WorkflowJobRunner({
      runner: runner,
      backend: backend,
      job: job,
      trace: false
    });
    t.ok(wf_job_runner, 'wf_job_runner ok');
    backend.runJob(job.uuid, runner.uuid, function (err, job) {
      t.ifError(err, 'backend.runJob error');
      wf_job_runner.run(function (err) {
        t.ifError(err, 'wf_job_runner run error');
        backend.getJob(job.uuid, function (err, job) {
          t.ifError(err, 'get job error');
          t.equal(job.execution, 'failed', 'job execution');
          t.equal(job.chain_results[0].error, 'Fail task error');
          t.end();
        });
      });
    });
  });
});


test('run a job which re-queues itself', function (t) {
  factory.job({
    workflow: reQueueWf.uuid,
    exec_after: '2012-01-03T12:54:05.788Z'
  }, function (err, job) {
    t.ifError(err, 'job error');
    t.ok(job, 'run job ok');
    wf_job_runner = new WorkflowJobRunner({
      runner: runner,
      backend: backend,
      job: job,
      trace: false
    });
    t.ok(wf_job_runner, 'wf_job_runner ok');
    backend.runJob(job.uuid, runner.uuid, function (err, job) {
      t.ifError(err, 'backend.runJob error');
      wf_job_runner.run(function (err) {
        t.ifError(err, 'wf_job_runner run error');
        backend.getJob(job.uuid, function (err, job) {
          t.ifError(err, 'backend.getJob error');
          t.ok(job, 'job ok');
          t.ok(job.elapsed, 'elapsed secs ok');
          elapsed = job.elapsed;
          t.equal(job.execution, 'queued', 'execution ok');
          t.equal(job.chain_results.length, 2, 'chain results ok');
          t.equal(job.chain_results[1].result, 'OK', 'result ok');
          t.equal(job.chain_results[1].error, 'queue', 'error ok');
          reQueuedJob = job;
          t.end();
        });
      });

    });
  });
});

test('run a previously re-queued job', function (t) {
  wf_job_runner = new WorkflowJobRunner({
    runner: runner,
    backend: backend,
    job: reQueuedJob,
    trace: false
  });
  t.ok(wf_job_runner, 'wf_job_runner ok');
  backend.runJob(reQueuedJob.uuid, runner.uuid, function (err, job) {
    t.ifError(err, 'backend.runJob error');
    wf_job_runner.run(function (err) {
      t.ok(
        wf_job_runner.timeout < (reQueuedJob.timeout * 1000),
        'elapsed timeout');
      t.ifError(err, 'wf_job_runner run error');
      backend.getJob(reQueuedJob.uuid, function (err, job) {
        t.ifError(err, 'backend.getJob error');
        t.ok(job, 'job ok');
        t.equal(job.execution, 'succeeded');
        t.equal(job.chain_results.length, 3);
        t.equal(job.chain_results[2].result, 'OK');
        t.ifError(job.chain_results[2].error);
        t.end();
      });
    });
  });
});


test('run a job which time out without "onerror"', function (t) {
  factory.job({
    workflow: timeoutWf.uuid,
    exec_after: '2012-01-03T12:54:05.788Z'
  }, function (err, job) {
    t.ifError(err, 'job error');
    t.ok(job, 'job ok');
    wf_job_runner = new WorkflowJobRunner({
      runner: runner,
      backend: backend,
      job: job,
      trace: false
    });
    t.ok(wf_job_runner, 'wf_job_runner ok');
    backend.runJob(job.uuid, runner.uuid, function (err, job) {
      t.ifError(err, 'backend.runJob error');
      wf_job_runner.run(function (err) {
        t.ifError(err, 'wf_job_runner run error');
        backend.getJob(job.uuid, function (err, job) {
          t.ifError(err, 'get job error');
          t.equal(job.execution, 'failed', 'job execution');
          t.equal(job.chain_results[0].error, 'workflow timeout');
          t.end();
        });
      });
    });
  });
});


test('a failed workflow with successful "onerror"', function (t) {
  factory.workflow({
    name: 'Failed wf with onerror ok',
    timeout: 0,
    chain: [ {
      name: 'A name',
      body: function (job, cb) {
        job.foo = 'This will fail';
        return cb('This will fail');
      }
    }],
    onerror: [ {
      name: 'A name',
      body: function (job, cb) {
        if (job.foo && job.foo === 'This will fail') {
          job.foo = 'OK!, expected failure. Fixed.';
          return cb();
        } else {
          return cb('Unknown failure');
        }
      }
    }]
  }, function (err, wf) {
    t.ifError(err, 'wf error');
    t.ok(wf, 'wf ok');
    factory.job({
      workflow: wf.uuid,
      exec_after: '2012-01-03T12:54:05.788Z'
    }, function (err, job) {
      t.ifError(err, 'job error');
      t.ok(job, 'job ok');
      wf_job_runner = new WorkflowJobRunner({
        runner: runner,
        backend: backend,
        job: job,
        trace: false
      });
      t.ok(wf_job_runner, 'wf_job_runner ok');
      t.equal(wf_job_runner.timeout, null, 'no runner timeout');
      t.equal(typeof (job.timeout), 'undefined', 'no job timeout');
      backend.runJob(job.uuid, runner.uuid, function (err, job) {
        t.ifError(err, 'backend.runJob error');
        wf_job_runner.run(function (err) {
          t.ifError(err, 'wf_job_runner run error');
          backend.getJob(job.uuid, function (err, job) {
            t.ifError(err, 'get job error');
            t.equal(job.execution, 'succeeded', 'job execution');
            t.ok(util.isArray(job.chain_results), 'chain results array');
            t.ok(util.isArray(job.onerror_results), 'onerror results array');
            t.equal(job.chain_results[0].error, 'This will fail');
            t.ifError(job.onerror_results[0].error, 'onerror_results error');
            t.ok(job.foo, 'job task added property ok');
            t.equal(job.foo, 'OK!, expected failure. Fixed.', 'job prop ok');
            t.ok(job.chain_results[0].started_at);
            t.ok(job.chain_results[0].finished_at);
            t.ok(job.onerror_results[0].started_at);
            t.ok(job.onerror_results[0].finished_at);
            t.end();
          });
        });
      });
    });
  });
});


test('a failed workflow with a non successful "onerror"', function (t) {
  factory.workflow({
    name: 'Failed wf with onerror not ok',
    chain: [ {
      name: 'A name',
      body: function (job, cb) {
        job.foo = 'Something else';
        return cb('This will fail');
      }
    }],
    onerror: [ {
      name: 'A name',
      body: function (job, cb) {
        if (job.foo && job.foo === 'This will fail') {
          job.foo = 'OK!, expected failure. Fixed.';
          return cb();
        } else {
          return cb('Unknown failure');
        }
      }
    }]
  }, function (err, wf) {
    t.ifError(err, 'wf error');
    t.ok(wf, 'wf ok');
    factory.job({
      workflow: wf.uuid,
      exec_after: '2012-01-03T12:54:05.788Z'
    }, function (err, job) {
      t.ifError(err, 'job error');
      t.ok(job, 'job ok');
      wf_job_runner = new WorkflowJobRunner({
        runner: runner,
        backend: backend,
        job: job,
        trace: false
      });
      t.ok(wf_job_runner, 'wf_job_runner ok');
      backend.runJob(job.uuid, runner.uuid, function (err, job) {
        t.ifError(err, 'backend.runJob error');
        wf_job_runner.run(function (err) {
          t.ifError(err, 'wf_job_runner run error');
          backend.getJob(job.uuid, function (err, job) {
            t.ifError(err, 'get job error');
            t.equal(job.foo, 'Something else', 'job prop ok');
            t.equal(job.chain_results[0].error, 'This will fail');
            t.equal(
              job.onerror_results[0].error,
              'Unknown failure',
              'onerror_results error');
            t.end();
          });
        });
      });
    });
  });
});


test('a job cannot access undefined sandbox modules', function (t) {
  factory.workflow({
    name: 'with undefined sandbox',
    chain: [ {
      name: 'A name',
      body: function (job, cb) {
        job.uuid = uuid();
        return cb(null);
      }
    }]
  }, function (err, wf) {
    t.ifError(err, 'wf error');
    t.ok(wf, 'wf ok');
    factory.job({
      workflow: wf.uuid,
      exec_after: '2012-01-03T12:54:05.788Z'
    }, function (err, job) {
      t.ifError(err, 'job error');
      t.ok(job, 'job ok');
      wf_job_runner = new WorkflowJobRunner({
        runner: runner,
        backend: backend,
        job: job,
        trace: false
      });
      t.ok(wf_job_runner, 'wf_job_runner ok');
      backend.runJob(job.uuid, runner.uuid, function (err, job) {
        t.ifError(err, 'backend.runJob error');
        wf_job_runner.run(function (err) {
          t.ifError(err, 'wf_job_runner run error');
          backend.getJob(job.uuid, function (err, job) {
            t.ifError(err);
            t.ok(job);
            t.equal(job.execution, 'failed');
            t.ok(job.chain_results[0].error);
            t.ok(job.chain_results[0].error.match(/uuid/gi));
            t.end();
          });
        });
      });
    });
  });
});


test('a job can access explicitly defined sandbox modules', function (t) {
  factory.workflow({
    name: 'explicitly defined sandbox',
    chain: [ {
      name: 'A name',
      body: function (job, cb) {
        job.uuid = uuid();
        return cb(null);
      }
    }]
  }, function (err, wf) {
    t.ifError(err, 'wf error');
    t.ok(wf, 'wf ok');
    factory.job({
      workflow: wf.uuid,
      exec_after: '2012-01-03T12:54:05.788Z'
    }, function (err, job) {
      t.ifError(err, 'job error');
      t.ok(job, 'job ok');
      wf_job_runner = new WorkflowJobRunner({
        runner: runner,
        backend: backend,
        job: job,
        trace: false,
        sandbox: {
          uuid: 'node-uuid'
        }
      });
      t.ok(wf_job_runner, 'wf_job_runner ok');
      backend.runJob(job.uuid, runner.uuid, function (err, job) {
        t.ifError(err, 'backend.runJob error');
        wf_job_runner.run(function (err) {
          t.ifError(err, 'wf_job_runner run error');
          backend.getJob(job.uuid, function (err, job) {
            t.ifError(err);
            t.ok(job);
            t.equal(job.execution, 'succeeded');
            t.ifError(job.chain_results[0].error);
            t.end();
          });
        });
      });
    });
  });

});


test('a canceled job', function (t) {
  factory.workflow({
    name: 'Job will be canceled',
    chain: [ {
      name: 'Timeout Task',
      timeout: 2,
      body: function (job, cb) {
        setTimeout(function () {
          // Should not be called:
          return cb('Error within timeout');
        }, 3050);
      },
      retry: 3
    }],
    onerror: [ {
      name: 'A name',
      body: function (job, cb) {
        return cb('It should not run');
      }
    }]
  }, function (err, wf) {
    t.ifError(err, 'wf error');
    t.ok(wf, 'wf ok');
    factory.job({
      workflow: wf.uuid,
      exec_after: '2012-01-03T12:54:05.788Z'
    }, function (err, job) {
      t.ifError(err, 'job error');
      t.ok(job, 'job ok');
      wf_job_runner = new WorkflowJobRunner({
        runner: runner,
        backend: backend,
        job: job,
        trace: false
      });
      t.ok(wf_job_runner, 'wf_job_runner ok');
      backend.runJob(job.uuid, runner.uuid, function (err, job) {
        t.ifError(err, 'backend.runJob error');
        setTimeout(function () {
          backend.updateJobProperty(
            job.uuid,
            'execution',
            'canceled',
            function (err) {
              t.ifError(err, 'updateJobProperty error');
            });
        }, 750);
        wf_job_runner.run(function (err) {
          t.ifError(err, 'wf_job_runner run error');
          backend.getJob(job.uuid, function (err, job) {
            t.ifError(err, 'get job error');
            t.ok(job, 'get job ok');
            t.equal(job.execution, 'canceled');
            t.ok(job.chain_results[0].error, 'task fails');
            t.equal(job.onerror_results.length, 0, 'should not call onerror');
            t.end();
          });
        });
      });
    });
  });

});


test('teardown', function (t) {
  backend.quit(function () {
    t.end();
  });
});
