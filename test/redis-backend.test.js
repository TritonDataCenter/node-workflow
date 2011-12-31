// Copyright 2012 Pedro P. Candel <kusorbox@gmail.com>. All rights reserved.
var test = require('tap').test,
    uuid = require('node-uuid'),
    SOCKET = '/tmp/.' + uuid(),
    util = require('util'),
    Factory = require('../lib/index').Factory,
    WorkflowRedisBackend = require('../lib/workflow-redis-backend');

var backend, factory;

var aTask, aWorkflow, aJob, anotherTask, fallbackTask;

var runnerId = uuid();

// A DB for testing, flushed before and right after we're done with tests
var TEST_DB_NUM = 15;

test('setup', function(t) {
  backend = new WorkflowRedisBackend({
    port: 6379,
    host: '127.0.0.1'
  });
  t.ok(backend, 'backend ok');
  backend.init(function() {
    backend.client.select(TEST_DB_NUM, function(err, res) {
      t.ifError(err, 'select db error');
      t.equal('OK', res, 'select db ok');
    });
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
    // wtf?
    // t.ok(backend.connected, 'backend connected');
    t.end();
  });
});


test('add a task', function(t) {
  factory.task('A Task', {
    timeout: 30,
    retry: 3
  }, function(job) {
    var self = this;
    job.foo = 1;
    this.emit('end');
  }, function(err, task) {
    t.ifError(err, 'task error');
    t.ok(task, 'task ok');
    aTask = task;
    backend.getTask(task.uuid, function(err, task) {
      t.ifError(err, 'backend.getTask error');
      t.ok(task, 'backend.getTask ok');
      t.equal(task.uuid, aTask.uuid);
      t.equal(task.body, aTask.body.toString());
      t.equal(task.name, aTask.name);
      t.end();
    });
  });
});

test('task name must be unique', function(t) {
  factory.task('A Task', {
    retry: 0
  }, function(job) {
    return 1;
  }, function(err, task) {
    t.ok(err, 'duplicated task name error');
    t.end();
  });
});

test('update a task', function(t) {
  aTask.retries = 4;
  aTask.name = 'A Task Name';
  backend.updateTask(aTask, function(err, task) {
    t.ifError(err, 'update task error');
    t.ok(task, 'task ok');
    backend.getTask(task.uuid, function(err, task) {
      t.ifError(err, 'backend get updated Task error');
      t.ok(task, 'backend get updated Task ok');
      t.equal(task.uuid, aTask.uuid);
      t.equal(task.body, aTask.body.toString());
      t.equal(task.name, aTask.name);
      t.end();
    });
  });
});

test('add more tasks', function(t) {
  // We need more than one task for the workflow stuff:
  factory.task('Another task', {}, function(job) {
    return 1;
  }, function(err, task) {
    t.ifError(err, 'another task error');
    t.ok(task, 'another task ok');
    anotherTask = task;
    factory.task('Fallback task', {}, function(job) {
      console.log('Workflow error');
    }, function(err, task) {
      t.ifError(err, 'fallback task error');
      t.ok(task, 'fallback task ok');
      fallbackTask = task;
      t.end();
    });
  });
});

test('add a workflow', function(t) {
  factory.workflow('A workflow', {
    chain: [aTask],
    timeout: 3,
    onError: [fallbackTask]
  }, function(err, workflow) {
    t.ifError(err, 'add workflow error');
    t.ok(workflow);
    aWorkflow = workflow;
    t.equal(workflow.chain[0], aTask.uuid);
    t.equal(workflow.onerror[0], fallbackTask.uuid);
    t.end();
  });
});

test('workflow name must be unique', function(t) {
  factory.workflow('A workflow', {
    chain: [aTask],
    timeout: 3,
    onError: [fallbackTask]
  }, function(err, workflow) {
    t.ok(err, 'duplicated workflow name err');
    t.end();
  });
});

test('update workflow', function(t) {
  aWorkflow.chain.push(anotherTask);
  aWorkflow.name = 'A workflow name';
  backend.updateWorkflow(aWorkflow, function(err, workflow) {
    t.ifError(err, 'update workflow error');
    t.ok(workflow);
    t.equal(workflow.chain[1], anotherTask.uuid);
    t.end();
  });
});


test('remove task from workflow', function(t) {
  factory.removeWorkflowTask(aWorkflow, anotherTask, function(err, workflow) {
    t.ifError(err, 'remove task error');
    t.ok(workflow, 'remove task workflow');
    aWorkflow = workflow;
    t.equal(aWorkflow.chain.length, 1);
    t.end();
  });
});


test('add task to workflow', function(t) {
  factory.addWorkflowTask(aWorkflow, anotherTask, function(err, workflow) {
    t.ifError(err, 'add task error');
    t.ok(workflow, 'add task workflow');
    aWorkflow = workflow;
    t.equal(workflow.chain[1], anotherTask.uuid);
    t.end();
  });
});

test('create job', function(t) {
  factory.job(aWorkflow, {
    target: '/foo/bar',
    params: {
      a: '1',
      b: '2'
    }
  }, function(err, job) {
    t.ifError(err, 'create job error');
    t.ok(job, 'create job ok');
    t.ok(job.exec_after);
    t.equal(job.status, 'queued');
    t.ok(job.uuid);
    t.equal(typeof job.chain, 'string');
    t.equal(typeof job.onerror, 'string');
    t.equal(typeof JSON.parse(job.chain), 'object');
    t.equal(typeof JSON.parse(job.onerror), 'object');
    aJob = job;
    t.end();
  });
});

test('duplicated job target', function(t) {
  factory.job(aWorkflow, {
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
  factory.job(aWorkflow, {
    target: '/foo/bar',
    params: {
      a: '2',
      b: '1'
    }
  }, function(err, job) {
    t.ifError(err, 'create job error');
    t.ok(job, 'create job ok');
    t.ok(job.exec_after);
    t.equal(job.status, 'queued');
    t.ok(job.uuid);
    t.equal(typeof job.chain, 'string');
    t.equal(typeof job.onerror, 'string');
    t.equal(typeof JSON.parse(job.chain), 'object');
    t.equal(typeof JSON.parse(job.onerror), 'object');
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
      t.notEqual(aJob.uuid, job.uuid);
      backend.getJob(aJob.uuid, function(err, job) {
        t.ifError(err, 'run job getJob');
        t.equal(job.runner, runnerId);
        t.equal(job.status, 'running');
        aJob = job;
        t.end();
      });
    });
  });
});


test('teardown', function(t) {
  // None of the deleteWorkflow/deleteTask calls are needed since we are
  // flushing the test db before we get started again.
  // Better we remove once we are done with tests for those, specially:
  // - What happens if a task is deleted and there is a workflow pointing
  //   to such task?. This seems to reveal we need to keep a per task index
  //   of where that task has been used (workflows uuids) and, in case we
  //   delete it, we remove it from every one of those workflows?
  backend.deleteWorkflow(aWorkflow, function(err, result) {
    t.ifError(err, 'delete workflow error');
    backend.deleteTask(aTask, function(err, result) {
      t.ifError(err, 'delete aTask error');
      backend.deleteTask(anotherTask, function(err, result) {
        t.ifError(err, 'delete anotherTask error');
        backend.deleteTask(fallbackTask, function(err, result) {
          t.ifError(err, 'delete fallbackTask error');
          backend.quit(function() {
            t.end();
          });
        });
      });
    });
  });
});

