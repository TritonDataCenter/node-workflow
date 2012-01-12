// Copyright 2012 Pedro P. Candel <kusorbox@gmail.com>. All rights reserved.

// First part of the example: You create workflows, add tasks, queue jobs
// anywhere in your code using NodeJS.

var assert = require('assert');

// With modules, it would be require('workflow');
var Factory = require('./lib/index').Factory,
    WorkflowRedisBackend = require('./lib/workflow-redis-backend');

var backend, factory;

// Some globals:
var aFallbackTask, aTask, anotherTask, aTaskWhichWillFail, aWorkflow, aJob;

// We'll use 'async' module to simplify definitions a bit, and avoid nesting
// stuff for clarity:
var async = require('async');

// Our serie of functions to execute:
var series = [

  function(callback) {
    // A task to be called on error:
    factory.task({
      name: 'A fallback task',
      body: function(job, cb) {
        // Some task failed and, as a consequence, this task is being executed
        if (job.error) {
          console.error(job.error);
        }
      }
    }, function(err, task) {
      if (err) {
        callback(err);
      }
      aFallbackTask = task;
      callback(null, task);
    });
  },

  function(callback) {
    // A task, that's it. It will fail on first retry, but succeed on 2nd one:
    factory.task({
      name: 'A Task',
      timeout: 30,
      retry: 2,
      body: function(job, cb) {
        if (!job.foo) {
          job.foo = true;
          return cb('Foo was not defined');
        }
        return cb(null);
      }
    }, function(err, task) {
      if (err) {
        callback(err);
      }
      aTask = task;
      callback(null, task);
    });
  },

  function(callback) {
    // This task will fail, but it will succeed when the task's onerror function
    // is called:
    factory.task({
      name: 'Another task',
      body: function(job, cb) {
        return cb('Task body error');
      },
      // Note that the `onerror` function takes the error as its first argument:
      onerror: function(err, job, cb) {
        job.the_err = err;
        return cb(null);
      }
    }, function(err, task) {
      if (err) {
        callback(err);
      }
      anotherTask = task;
      callback(null, task);
    });
  },

  function(callback) {
    // This task will fail and, given there isn't an onerror callback,
    // the workflow will call the `onerror` chain:
    factory.task({
      name: 'A task which will fail',
      body: function(job, cb) {
        job.this_failed_because = 'We decided it.';
        return cb('Task body error');
      }
    }, function(err, task) {
      if (err) {
        callback(err);
      }
      aTaskWhichWillFail = task;
      callback(null, task);
    });
  },

  function(callback) {
    // A workflow definition:
    factory.workflow({
      name: 'Sample Workflow',
      chain: [aTask, anotherTask, aTaskWhichWillFail],
      timeout: 3,
      onError: [aFallbackTask]
    }, function(err, workflow) {
      if (err) {
        callback(err);
      }
      aWorkflow = workflow;
      callback(null, workflow);
    });
  },

  function(callback) {
    // A Job based on the workflow:
    factory.job({
      workflow: aWorkflow,
      exec_after: '2012-01-03T12:54:05.788Z'
    }, function(err, job) {
      if (err) {
        callback(err);
      }
      aJob = job;
      callback(null, job);
    });
  }

];


function main() {
  // A DB for testing, flushed before and right after we're done with tests
  var TEST_DB_NUM = 15;

  backend = new WorkflowRedisBackend({
    port: 6379,
    host: '127.0.0.1'
  });

  backend.init(function() {
    backend.client.select(TEST_DB_NUM, function(err, res) {
      assert.ifError(err);
      assert.equal('OK', res);
    });

    backend.client.flushdb(function(err, res) {
      assert.ifError(err);
      assert.equal('OK', res);
    });

    backend.client.dbsize(function(err, res) {
      assert.ifError(err);
      assert.equal(0, res);
    });

    factory = Factory(backend);
    assert.ok(factory);

    async.series(series, function(err, results) {
      if (err) {
        console.error(err);
        return;
      }
      // At this point, we should have a results array with all the tasks,
      // the workflow and the job, on the same order we defined them but, given
      // we've set the objects to globals, we couldn't care less about this
      // async's results array.
      //
      // Our tasks and workflow should have been created, and our job should
      // have been created and queued:
      assert.ok(aTask);
      assert.ok(aTask.uuid);
      assert.ok(anotherTask);
      assert.ok(anotherTask.uuid);
      assert.ok(aFallbackTask);
      assert.ok(aFallbackTask.uuid);
      assert.ok(aTaskWhichWillFail);
      assert.ok(aTaskWhichWillFail.uuid);
      assert.ok(aWorkflow);
      assert.ok(aWorkflow.uuid);
      assert.ok(aJob);
      // We need the UUID in order to be able to check Job Status
      assert.ok(aJob.uuid);
      console.log(aJob);
    });
  });
}

main();

