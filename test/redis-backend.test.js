// Copyright 2012 Pedro P. Candel <kusorbox@gmail.com>. All rights reserved.
var test = require('tap').test,
    uuid = require('node-uuid'),
    SOCKET = '/tmp/.' + uuid(),
    util = require('util'),
    Factory = require('../lib/index').Factory,
    WorkflowRedisBackend = require('../lib/workflow-redis-backend');

var backend, factory;

var aWorkflow, aJob, anotherJob;

var runnerId = uuid();

// A DB for testing, flushed before and right after we're done with tests
var TEST_DB_NUM = 15;

test('setup', function(t) {
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
    factory = Factory(backend);
    t.ok(factory, 'factory ok');
    t.end();
  });
});


test('add a workflow', function(t) {
  factory.workflow({
    name: 'A workflow',
    chain: [{
      name: 'A Task',
      timeout: 30,
      retry: 3,
      body: function(job, cb) {
        return cb(null);
      }
    }],
    timeout: 180,
    onError: [{
      name: 'Fallback task',
      body: function(job, cb) {
        return cb('Workflow error');
      }
    }]
  }, function(err, workflow) {
    t.ifError(err, 'add workflow error');
    t.ok(workflow, 'add workflow ok');
    aWorkflow = workflow;
    t.ok(workflow.chain[0].uuid, 'add workflow chain task');
    t.ok(workflow.onerror[0].uuid, 'add workflow onerror task');
    t.end();
  });
});

test('workflow name must be unique', function(t) {
  factory.workflow({
    name: 'A workflow',
    chain: [{
      name: 'A Task',
      timeout: 30,
      retry: 3,
      body: function(job, cb) {
        return cb(null);
      }
    }],
    timeout: 180,
    onError: [{
      name: 'Fallback task',
      body: function(job, cb) {
        return cb('Workflow error');
      }
    }]
  }, function(err, workflow) {
    t.ok(err, 'duplicated workflow name err');
    t.end();
  });
});

test('update workflow', function(t) {
  aWorkflow.chain.push({
    name: 'Another task',
    body: function(job, cb) {
      return cb(null);
    }.toString()
  });
  aWorkflow.name = 'A workflow name';
  backend.updateWorkflow(aWorkflow, function(err, workflow) {
    t.ifError(err, 'update workflow error');
    t.ok(workflow, 'update workflow ok');
    t.ok(workflow.chain[1].name, 'Updated task ok');
    t.ok(workflow.chain[1].body, 'Updated task body ok');
    t.end();
  });
});


test('create job', function(t) {
  factory.job({
    workflow: aWorkflow.uuid,
    target: '/foo/bar',
    params: {
      a: '1',
      b: '2'
    }
  }, function(err, job) {
    t.ifError(err, 'create job error');
    t.ok(job, 'create job ok');
    t.ok(job.exec_after, 'job exec_after');
    t.equal(job.execution, 'queued', 'job queued');
    t.ok(job.uuid, 'job uuid');
    t.ok(util.isArray(job.chain), 'job chain is array');
    t.ok(util.isArray(job.onerror), 'job onerror is array');
    t.ok(
      (typeof job.params === 'object' && !util.isArray(job.params)),
      'params ok'
    );
    aJob = job;
    t.end();
  });
});

test('duplicated job target', function(t) {
  factory.job({
    workflow: aWorkflow.uuid,
    target: '/foo/bar',
    params: {
      a: '1',
      b: '2'
    }
  }, function(err, job) {
    t.ok(err, 'duplicated job error');
    t.end();
  });
});


test('job with different params', function(t) {
  factory.job({
    workflow: aWorkflow.uuid,
    target: '/foo/bar',
    params: {
      a: '2',
      b: '1'
    }
  }, function(err, job) {
    console.log(err);
    t.ifError(err, 'create job error');
    t.ok(job, 'create job ok');
    t.ok(job.exec_after);
    t.equal(job.execution, 'queued');
    t.ok(job.uuid);
    t.ok(util.isArray(job.chain), 'job chain is array');
    t.ok(util.isArray(job.onerror), 'job onerror is array');
    t.ok(
      (typeof job.params === 'object' && !util.isArray(job.params)),
      'params ok'
    );
    anotherJob = job;
    t.end();
  });
});


test('next jobs', function(t) {
  backend.nextJobs(0, 1, function(err, jobs) {
    t.ifError(err, 'next jobs error');
    t.equal(jobs.length, 2);
    t.equal(jobs[0], aJob.uuid);
    t.equal(jobs[1], anotherJob.uuid);
    t.end();
  });
});


test('next queued job', function(t) {
  var idx = 0;
  backend.nextJob(function(err, job) {
    t.ifError(err, 'next job error' + idx);
    idx += 1;
    t.ok(job, 'first queued job OK');
    t.equal(aJob.uuid, job.uuid);
    backend.nextJob(idx, function(err, job) {
      t.ifError(err, 'next job error: ' + idx);
      idx += 1;
      t.ok(job, '2nd queued job OK');
      t.notEqual(aJob.uuid, job.uuid);
      backend.nextJob(idx, function(err, job) {
        t.ifError(err, 'next job error: ' + idx);
        t.equal(job, null, 'no more queued jobs');
        t.end();
      });
    });
  });
});


test('run job', function(t) {
  backend.runJob(aJob.uuid, runnerId, function(err) {
    t.ifError(err, 'run job error');
    // If the job is running, it shouldn't be available for nextJob:
    backend.nextJob(function(err, job) {
      t.ifError(err, 'run job next error');
      t.notEqual(aJob.uuid, job.uuid, 'run job next job');
      backend.getJob(aJob.uuid, function(err, job) {
        t.ifError(err, 'run job getJob');
        t.equal(job.runner, runnerId, 'run job runner');
        t.equal(job.execution, 'running', 'run job status');
        aJob = job;
        t.end();
      });
    });
  });
});


test('update job', function(t) {
  aJob.chain_results = [
    {result: 'OK', error: ''},
    {result: 'OK', error: ''}
  ];

  backend.updateJob(aJob, function(err) {
    t.ifError(err, 'update job error');
    backend.getJob(aJob.uuid, function(err, job) {
      t.ifError(err, 'update job getJob');
      t.equal(job.runner, runnerId, 'update job runner');
      t.equal(job.execution, 'running', 'update job status');
      t.ok(util.isArray(job.chain_results), 'chain_results is array');
      t.equal(2, job.chain_results.length);
      aJob = job;
      t.end();
    });

  });
});


test('finish job', function(t) {
  aJob.chain_results = [
    {result: 'OK', error: ''},
    {result: 'OK', error: ''},
    {result: 'OK', error: ''},
    {result: 'OK', error: ''}
  ];

  backend.finishJob(aJob, function(err) {
    t.ifError(err, 'finish job error');
    backend.getJob(aJob.uuid, function(err, job) {
      t.ifError(err, 'finish job getJob error');
      t.deepEqual(job.chain_results, aJob.chain_results, 'finish job results');
      t.ok(!job.runner);
      t.equal(job.execution, 'succeeded', 'finished job status');
      t.end();
    });
  });
});


test('re queue job', function(t) {
  backend.runJob(anotherJob.uuid, runnerId, function(err) {
    t.ifError(err, 're queue job run job error');
    anotherJob.chain_results = JSON.stringify([
      {success: true, error: ''}
    ]);
    backend.queueJob(anotherJob, function(err) {
      t.ifError(err, 're queue job error');
      backend.getJob(anotherJob.uuid, function(err, job) {
        t.ifError(err, 're queue job getJob');
        t.ok(!job.runner, 're queue job runner');
        t.equal(job.execution, 'queued', 're queue job status');
        anotherJob = job;
        t.end();
      });
    });
  });
});


test('register runner', function(t) {
  var d = new Date();
  backend.registerRunner(runnerId, function(err) {
    t.ifError(err, 'register runner error');
    backend.client.hget('wf_runners', runnerId, function(err, res) {
      t.ifError(err, 'get runner error');
      t.ok((new Date(res).getTime() >= d.getTime()), 'runner timestamp');
      t.end();
    });
  });
});


test('runner active', function(t) {
  var d = new Date();
  backend.runnerActive(runnerId, function(err) {
    t.ifError(err, 'runner active error');
    backend.client.hget('wf_runners', runnerId, function(err, res) {
      t.ifError(err, 'get runner error');
      t.ok((new Date(res).getTime() >= d.getTime()), 'runner timestamp');
      t.end();
    });
  });
});


test('get all runners', function(t) {
  backend.getRunners(function(err, runners) {
    t.ifError(err, 'get runners error');
    t.ok(runners, 'runners ok');
    t.ok(runners[runnerId], 'runner id ok');
    t.ok(new Date(runners[runnerId]), 'runner timestamp ok');
    t.end();
  });
});


test('get workflows', function(t) {
  backend.getWorkflows(function(err, workflows) {
    t.ifError(err, 'get workflows error');
    t.ok(workflows, 'workflows ok');
    t.equal(workflows[0].uuid, aWorkflow.uuid, 'workflow uuid ok');
    t.ok(util.isArray(workflows[0].chain), 'workflow chain ok');
    t.ok(util.isArray(workflows[0].onerror), 'workflow onerror ok');
    t.end();
  });
});


test('get all jobs', function(t) {
  backend.getJobs(function(err, jobs) {
    t.ifError(err, 'get all jobs error');
    t.ok(jobs, 'jobs ok');
    t.ok(util.isArray(jobs[0].chain), 'jobs chain ok');
    t.ok(util.isArray(jobs[0].onerror), 'jobs onerror ok');
    t.ok(util.isArray(jobs[0].chain_results), 'jobs chain_results ok');
    t.ok(
      (typeof jobs[0].params === 'object' && !util.isArray(jobs[0].params)),
      'job params ok'
    );
    t.equal(jobs.length, 2);
    t.end();
  });
});


test('get succeeded jobs', function(t) {
  backend.getJobs('succeeded', function(err, jobs) {
    t.ifError(err, 'get succeeded jobs error');
    t.ok(jobs, 'jobs ok');
    t.equal(jobs.length, 1);
    t.equal(jobs[0].execution, 'succeeded');
    t.ok(util.isArray(jobs[0].chain), 'jobs chain ok');
    t.ok(util.isArray(jobs[0].onerror), 'jobs onerror ok');
    t.ok(util.isArray(jobs[0].chain_results), 'jobs chain_results ok');
    t.ok(
      (typeof jobs[0].params === 'object' && !util.isArray(jobs[0].params)),
      'job params ok'
    );
    t.end();
  });
});


test('get queued jobs', function(t) {
  backend.getJobs('queued', function(err, jobs) {
    t.ifError(err, 'get queued jobs error');
    t.ok(jobs, 'jobs ok');
    t.equal(jobs.length, 1);
    t.equal(jobs[0].execution, 'queued');
    t.ok(util.isArray(jobs[0].chain), 'jobs chain ok');
    t.ok(util.isArray(jobs[0].onerror), 'jobs onerror ok');
    t.ok(util.isArray(jobs[0].chain_results), 'jobs chain_results ok');
    t.ok(
      (typeof jobs[0].params === 'object' && !util.isArray(jobs[0].params)),
      'job params ok'
    );
    t.end();
  });
});


test('teardown', function(t) {
  backend.quit(function() {
    t.end();
  });
});

