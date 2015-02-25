// Copyright 2013 Pedro P. Candel <kusorbox@gmail.com>. All rights reserved.

// First part of the example: You create workflows, add tasks, queue jobs
// anywhere in your code using NodeJS.

var assert = require('assert');
var util = require('util');
var wf = require('./lib/index');
// With modules, it would be require('workflow');
var Factory = wf.Factory;
var Backend = wf.MemoryBackend;

var backend, factory;

// Some globals:
var aWorkflow, aJob;

// We'll use 'vasync' module to simplify definitions a bit, and avoid nesting
// stuff for clarity:
var vasync = require('vasync');

// Our serie of functions to execute:
var pipeline = [

    function createWorkflow(_, callback) {
        // A workflow definition:
        factory.workflow({
            name: 'Sample Workflow',
            chain: [ {
                // A task. It will fail on first retry, but succeed on 2nd one:
                name: 'A Task',
                timeout: 30,
                retry: 2,
                body: function (job, cb) {
                    if (!job.foo) {
                        job.foo = true;
                        return cb('Foo was not defined');
                    }
                    return cb(null);
                }
            },
            {
                // This task will fail, but it will succeed when the task's
                // fallback function is called:
                name: 'Another task',
                body: function (job, cb) {
                    return cb('Task body error');
                },
                // Note that the `fallback` function takes the error as its
                // first argument:
                fallback: function (err, job, cb) {
                    job.the_err = err;
                    return cb(null);
                }
            },
            {
                // This task will fail and, given there isn't an fallback
                // callback, the workflow will call the `onerror` chain:
                name: 'A task which will fail',
                body: function (job, cb) {
                    job.this_failed_because = 'We decided it.';
                    return cb('Task body error');
                }
            }],
            timeout: 180,
            onError: [ {
                name: 'A fallback task',
                body: function (job, cb) {
                    // Some task failed and, as a consequence, this task is
                    // being executed
                    if (job.error) {
                        // Do something here ...
                        cb(job.error);
                    }
                    cb(null);
                }
            }]
        }, function (err, workflow) {
            if (err) {
                callback(err);
            }
            aWorkflow = workflow;
            callback(null, workflow);
        });
    },
    function createJob(_, callback) {
        // A Job based on the workflow:
        factory.job({
            workflow: aWorkflow.uuid,
            exec_after: '2012-01-03T12:54:05.788Z'
        }, function (err, job) {
            if (err) {
                callback(err);
            }
            aJob = job;
            callback(null, job);
        });
    }

];


function main() {
    backend = Backend({});

    backend.init(function () {
        factory = Factory(backend);
        assert.ok(factory);

        vasync.pipeline({
            funcs: pipeline
        }, function (err, results) {
            if (err) {
                console.error(err);
                return;
            }
            // At this point, we should have a results array with all the
            // tasks, the workflow and the job, on the same order we defined
            // them but,given we've set the objects to globals, we couldn't
            // care less about this async's results array.
            //
            // Our tasks and workflow should have been created, and our job
            // should have been created and queued:
            assert.ok(aWorkflow);
            assert.ok(aWorkflow.uuid);
            assert.ok(aJob);
            // We need the UUID in order to be able to check Job Status
            assert.ok(aJob.uuid);
            console.log(util.inspect(results.operations, false, 8));
        });
    });
}

main();
