// Copyright 2012 Pedro P. Candel <kusorbox@gmail.com>. All rights reserved.
var test = require('tap').test,
    uuid = require('node-uuid'),
    SOCKET = '/tmp/.' + uuid(),
    util = require('util'),
    Factory = require('../lib/index').Factory,
    WorkflowRedisBackend = require('../lib/workflow-redis-backend');

var backend, factory;

var aTask, aWorkflow, aJob, anotherTask, fallbackTask;

test('setup', function(t) {
  backend = new WorkflowRedisBackend({
    port: 6379,
    host: '127.0.0.1'
  });
  t.ok(backend, 'backend ok');
  backend.init(function() {
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

test('teardown', function(t) {
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

