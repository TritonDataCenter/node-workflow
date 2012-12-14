// Copyright 2012 Pedro P. Candel <kusorbox@gmail.com>. All rights reserved.
var uuid = require('node-uuid'),
    util = require('util');

var WorkflowFactory = module.exports = function (backend) {
    // Create a workflow and store it on the backend
    //
    // - workflow - the workflow object properties:
    //   - name: string workflow name, uniqueness enforced.
    //   - timeout: integer, acceptable time, in seconds, to run the wf.
    //     (60 minutes if nothing given). Also, the Boolean `false` can be used
    //     to explicitly create a workflow without a timeout.
    //   - chain: An array of Tasks to run.
    //   - onerror: An array of Tasks to run in case `chain` fails.
    // - opts - Object, any additional information to be passed to the backend
    //          when creating a workflow object which are not workflow
    //          properties, like HTTP request ID or other meta information.
    // - callback - function(err, workflow)
    //
    // Every Task can have the following members:
    //   - name - string task name, optional.
    //   - body - function(job, cb) the task main function. Required.
    //   - fallback: function(err, job, cb) a function to run in case `body`
    //     fails. Optional.
    //   - retry: Integer, number of attempts to run the task before try
    //     `fallback`. Optional. By default, just one retry.
    //   - timeout: Integer, acceptable time, in seconds, a task execution
    //     should take, before fail it with timeout error. Optional.
    //
    function workflow(w, opts, callback) {
        var wf = w || {};

        if (typeof (opts) === 'function') {
            callback = opts;
            opts = {};
        }

        function validateTask(task, cb) {
            var p;

            if (!task.body) {
                return cb('Task "body" is required');
            }

            if (typeof (task.body) !== 'function') {
                return cb('Task "body" must be a function');
            }

            if (!task.uuid) {
                task.uuid = uuid();
            }

            // Ensure that if task.fallback is given, it's a function
            if (task.fallback && typeof (task.fallback) !== 'function') {
                return cb('Task "fallback" must be a function');
            }

            // If task is overriding modules to load, make sure it's a
            // proper object with keys:
            if (task.modules &&
                    (typeof (task.modules) !== 'object' ||
                     !Array.isArray(Object.keys(task.modules)))) {
                return cb('Task "modules" must be an object');
            }
            for (p in task) {
                if (typeof (task[p]) === 'function') {
                    task[p] = task[p].toString();
                } else if (typeof (task[p]) === 'object') {
                    task[p] = JSON.stringify(task[p]);
                }
            }
            return task;
        }

        if (!wf.name) {
            return callback('Workflow "name" is required');
        }

        if (wf.chain && (
              typeof (wf.chain) !== 'object' ||
              typeof (wf.chain.length) === 'undefined')) {
            return callback('Workflow "chain" must be an array');
        }

        if (!wf.chain) {
            wf.chain = [];
        }

        if (!wf.uuid) {
            wf.uuid = uuid();
        }

        if (wf.onError) {
            wf.onerror = wf.onError;
            delete wf.onError;
        }

        if (wf.onerror && (
              typeof (wf.onerror) !== 'object' ||
              typeof (wf.onerror.length) === 'undefined')) {
            return callback('Workflow "onerror" must be an array');
        }

        wf.chain.forEach(function (task, i, arr) {
            wf.chain[i] = validateTask(task, callback);
        });

        if (wf.onerror) {
            wf.onerror.forEach(function (task, i, arr) {
                wf.onerror[i] = validateTask(task, callback);
            });
        }

        if (typeof (wf.timeout) !== 'number') {
            wf.timeout = 3600;
        } else if (wf.timeout === 0) {
            delete wf.timeout;
        }

        return backend.createWorkflow(wf, function (err, result) {
            if (err) {
                return callback(err);
            } else {
                return callback(null, wf);
            }
        });

    }
    // Create a queue a Job from the given Workflow:
    //
    // - j - the Job object workflow and extra arguments:
    //   - workflow - (required) UUID of Workflow object to create the job from.
    //   - params - (opt) JSON object, parameters to pass to the job during exec
    //   - target - (opt) String, Job's target, used to ensure that we don't
    //              queue two jobs with the same target and params at once.
    //   - exec_after - (opt) ISO 8601 Date, delay job execution after the
    //                  given timestamp (execute from now when not given).
    // - opts - Object, any additional information to be passed to the backend
    //          when creating a workflow object which are not workflow
    //          properties, like HTTP request ID or other meta information.
    // - callback - f(err, job)
    function job(j, opts, callback) {
        var theJob = { execution: 'queued', chain_results: []};

        if (!j.workflow) {
            return callback('"j.workflow" is required');
        }

        if (typeof (opts) === 'function') {
            callback = opts;
            opts = {};
        }

        return backend.getWorkflow(j.workflow, function (err, wf) {
            var p;
            if (err) {
                return callback(err);
            }

            if (Object.keys(wf).length === 0) {
                return callback(
                    'Cannot create a job from an unexisting workflow');
            }

            if (wf.chain.length === 0) {
                return callback(
                  'Cannot queue a job from a workflow without any task');
            }

            for (p in wf) {
                if (p !== 'uuid') {
                    theJob[p] = wf[p];
                } else {
                    theJob.workflow_uuid = wf.uuid;
                }
            }

            theJob.exec_after = j.exec_after || new Date().toISOString();
            theJob.params = j.params || {};

            if (!theJob.uuid) {
                theJob.uuid = uuid();
            }

            if (j.target) {
                theJob.target = j.target;
            }

            return backend.validateJobTarget(theJob, function (err) {
                if (err) {
                    return callback(err);
                } else {
                    return backend.createJob(theJob, function (err, results) {
                        if (err) {
                            return callback(err);
                        } else {
                            return callback(null, theJob);
                        }
                    });
                }
            });
        });
    }

    return {
        workflow: workflow,
        job: job
    };
};
