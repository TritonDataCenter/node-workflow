// Copyright 2012 Pedro P. Candel <kusorbox@gmail.com>. All rights reserved.

// Note the following database will not be created, and needs to exist before
// the tests can successfully run:
//
//    `create database node_workflow_test owner postgres;`
//
// or whatever the postgres user you want to run tests as.

var test = require('tap').test,
    uuid = require('node-uuid'),
    SOCKET = '/tmp/.' + uuid(),
    util = require('util'),
    async = require('async'),
    Factory = require('../lib/index').Factory,
    WorkflowPgBackend = require('../lib/workflow-pg-backend');

var backend, factory;

var aWorkflow, aJob, anotherJob;

var helper = require('./helper'),
    config = helper.config(),
    runnerId = config.runner.identifier;

var pg_opts = {
  database: 'node_workflow_test',
  test: true
};

test('setup', function (t) {
  backend = new WorkflowPgBackend(pg_opts);
  t.ok(backend, 'backend ok');
  backend.init(function (err) {
    t.ifError(err, 'backend init error');
    factory = Factory(backend);
    t.ok(factory, 'factory ok');
    t.end();
  });
});


test('add a workflow', function (t) {
  factory.workflow({
    name: 'A workflow',
    chain: [ {
      name: 'A Task',
      timeout: 30,
      retry: 3,
      body: function (job, cb) {
        return cb(null);
      }
    }],
    timeout: 180,
    onError: [ {
      name: 'Fallback task',
      body: function (job, cb) {
        return cb('Workflow error');
      }
    }]
  }, function (err, workflow) {
    t.ifError(err, 'add workflow error');
    t.ok(workflow, 'add workflow ok');
    aWorkflow = workflow;
    t.ok(workflow.chain[0].uuid, 'add workflow chain task');
    t.ok(workflow.onerror[0].uuid, 'add workflow onerror task');
    t.end();
  });
});


test('workflow name must be unique', function (t) {
  factory.workflow({
    name: 'A workflow',
    chain: [ {
      name: 'A Task',
      timeout: 30,
      retry: 3,
      body: function (job, cb) {
        return cb(null);
      }
    }],
    timeout: 180,
    onError: [ {
      name: 'Fallback task',
      body: function (job, cb) {
        return cb('Workflow error');
      }
    }]
  }, function (err, workflow) {
    t.ok(err, 'duplicated workflow name err');
    t.end();
  });
});


test('get workflow', function (t) {
  backend.getWorkflow(aWorkflow.uuid, function (err, workflow) {
    t.ifError(err, 'get workflow error');
    t.ok(workflow, 'get workflow ok');
    t.equivalent(workflow, aWorkflow);
    backend.getWorkflow(uuid(), function (err, workflow) {
      t.ok(err.match(/uuid/gi), 'unexisting workflow error');
      t.end();
    });
  });
});


test('update workflow', function (t) {
  aWorkflow.chain.push({
    name: 'Another task',
    body: function (job, cb) {
      return cb(null);
    }.toString()
  });
  aWorkflow.name = 'A workflow name';
  backend.updateWorkflow(aWorkflow, function (err, workflow) {
    t.ifError(err, 'update workflow error');
    t.ok(workflow, 'update workflow ok');
    t.ok(workflow.chain[1].name, 'Updated task ok');
    t.ok(workflow.chain[1].body, 'Updated task body ok');
    t.end();
  });
});

test('create job', function (t) {
  factory.job({
    workflow: aWorkflow.uuid,
    target: '/foo/bar',
    params: {
      a: '1',
      b: '2'
    }
  }, function (err, job) {
    t.ifError(err, 'create job error');
    t.ok(job, 'create job ok');
    t.ok(job.exec_after, 'job exec_after');
    t.equal(job.execution, 'queued', 'job queued');
    t.ok(job.uuid, 'job uuid');
    t.ok(util.isArray(job.chain), 'job chain is array');
    t.ok(util.isArray(job.onerror), 'job onerror is array');
    t.ok(
      (typeof (job.params) === 'object' && !util.isArray(job.params)),
      'params ok');
    aJob = job;
    backend.getJobProperty(aJob.uuid, 'target', function (err, val) {
      t.ifError(err, 'get job property error');
      t.equal(val, '/foo/bar', 'property value ok');
      t.end();
    });
  });
});


test('duplicated job target', function (t) {
  factory.job({
    workflow: aWorkflow.uuid,
    target: '/foo/bar',
    params: {
      a: '1',
      b: '2'
    }
  }, function (err, job) {
    t.ok(err, 'duplicated job error');
    t.end();
  });
});


test('job with different params', function (t) {
  // Just to make sure we can sort jobs by different timestamp:
  setTimeout(function () {
    factory.job({
      workflow: aWorkflow.uuid,
      target: '/foo/bar',
      params: {
        a: '2',
        b: '1'
      }
    }, function (err, job) {
      t.ifError(err, 'create job error');
      t.ok(job, 'create job ok');
      t.ok(job.exec_after);
      t.equal(job.execution, 'queued');
      t.ok(job.uuid);
      t.ok(util.isArray(job.chain), 'job chain is array');
      t.ok(util.isArray(job.onerror), 'job onerror is array');
      t.ok(
        (typeof (job.params) === 'object' && !util.isArray(job.params)),
        'params ok');
      anotherJob = job;
      t.end();
    });
  }, 1000);
});


test('next jobs', function (t) {
  backend.nextJobs(0, 1, function (err, jobs) {
    t.ifError(err, 'next jobs error');
    t.equal(jobs.length, 2);
    t.equal(jobs[0], aJob.uuid);
    t.equal(jobs[1], anotherJob.uuid);
    t.end();
  });
});


test('next queued job', function (t) {
  var idx = 0;
  backend.nextJob(function (err, job) {
    t.ifError(err, 'next job error' + idx);
    idx += 1;
    t.ok(job, 'first queued job OK');
    t.equal(aJob.uuid, job.uuid);
    backend.nextJob(idx, function (err, job) {
      t.ifError(err, 'next job error: ' + idx);
      idx += 1;
      t.ok(job, '2nd queued job OK');
      t.notEqual(aJob.uuid, job.uuid);
      backend.nextJob(idx, function (err, job) {
        t.ifError(err, 'next job error: ' + idx);
        t.equal(job, null, 'no more queued jobs');
        t.end();
      });
    });
  });
});


test('register runner', function (t) {
  var d = new Date();
  backend.registerRunner(runnerId, function (err) {
    t.ifError(err, 'register runner error');
    backend.getRunner(runnerId, function (err, res) {
      t.ifError(err, 'get runner error');
      t.ok((res.active_at.getTime() >= d.getTime()), 'runner timestamp');
      t.end();
    });
  });
  t.test('with specific time', function (t) {
    backend.registerRunner(runnerId, d.toISOString(), function (err) {
      t.ifError(err, 'register runner error');
      backend.getRunner(runnerId, function (err, res) {
        t.ifError(err, 'backend get runner error');
        t.equal(d.toISOString(), res.active_at);
        t.end();
      });
    });
  });
});


test('runner active', function (t) {
  var d = new Date();
  backend.runnerActive(runnerId, function (err) {
    t.ifError(err, 'runner active error');
    backend.getRunner(runnerId, function (err, res) {
      t.ifError(err, 'get runner error');
      t.ok((res.active_at.getTime() >= d.getTime()), 'runner timestamp');
      t.end();
    });
  });
});


test('get all runners', function (t) {
  backend.getRunners(function (err, runners) {
    t.ifError(err, 'get runners error');
    t.ok(runners, 'runners ok');
    t.ok(runners[0].uuid, 'runner id ok');
    t.ok(runners[0].active_at, 'runner timestamp ok');
    t.end();
  });
});


test('idle runner', function (t) {
  t.test('check runner is not idle', function (t) {
    backend.isRunnerIdle(runnerId, function (idle) {
      t.equal(idle, false);
      t.end();
    });
  });
  t.test('set runner as idle', function (t) {
    backend.idleRunner(runnerId, function (err) {
      t.ifError(err);
      t.end();
    });
  });
  t.test('check runner is idle', function (t) {
    backend.isRunnerIdle(runnerId, function (idle) {
      t.equal(idle, true);
      t.end();
    });
  });
  t.test('set runner as not idle', function (t) {
    backend.wakeUpRunner(runnerId, function (err) {
      t.ifError(err);
      t.end();
    });
  });
  t.test('check runner is not idle', function (t) {
    backend.isRunnerIdle(runnerId, function (idle) {
      t.equal(idle, false);
      t.end();
    });
  });
});



test('get workflows', function (t) {
  backend.getWorkflows(function (err, workflows) {
    t.ifError(err, 'get workflows error');
    t.ok(workflows, 'workflows ok');
    t.equal(workflows[0].uuid, aWorkflow.uuid, 'workflow uuid ok');
    t.ok(util.isArray(workflows[0].chain), 'workflow chain ok');
    t.ok(util.isArray(workflows[0].onerror), 'workflow onerror ok');
    t.end();
  });
});


test('delete workflow', function (t) {
  t.test('when the workflow exists', function (t) {
    backend.deleteWorkflow(aWorkflow, function (err, success) {
      t.ifError(err, 'delete existing workflow error');
      t.ok(success);
      t.end();
    });
  });
  t.test('when the workflow does not exist', function (t) {
    backend.deleteWorkflow(aWorkflow, function (err, success) {
      t.ifError(err, 'delete unexisting workflow error');
      t.equal(success, false, 'no row deleted');
      t.end();
    });
  });
  t.end();
});


test('teardown', function (t) {
  backend.client.query('DROP TABLE wf_workflows', function (err, res) {
    t.ifError(err, 'drop wf_workflows error');
    backend.client.query('DROP TABLE wf_jobs', function (err, res) {
      t.ifError(err, 'drop wf_jobs error');
      backend.quit(function () {
        t.end();
      });
    });
  });
});
