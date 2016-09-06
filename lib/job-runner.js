// Copyright 2014 Pedro P. Candel <kusorbox@gmail.com>. All rights reserved.

var assert = require('assert-plus');
var util = require('util');
var fork = require('child_process').fork;
var vasync = require('vasync');
var WorkflowTaskRunner = require('./task-runner');
var backoff = require('backoff');
var clone = require('clone');

// Run the given job. Optionally, can pass sandbox object for the 'task' VM
// - opts (Object) with the following members:
//
// - runner (Object) insteance of the runner running this job. Required to
//   notify the runner about child processes spawned/finished. Required.
// - backend (Object) instance of the backend used. Required.
// - job (Object) the job to run. Required.
// - log (Object) A bunyan logger. Required.
// - sandbox (Object) VM's sandbox for task (see WorkflowTaskRunner). Optional.
var WorkflowJobRunner = module.exports = function (opts) {
    if (typeof (opts) !== 'object') {
        throw new TypeError('opts (Object) required');
    }

    if (typeof (opts.runner) !== 'object') {
        throw new TypeError('opts.runner (Object) required');
    }

    if (typeof (opts.backend) !== 'object') {
        throw new TypeError('opts.backend (Object) required');
    }

    if (typeof (opts.job) !== 'object') {
        throw new TypeError('opts.job (Object) required');
    }

    if (opts.sandbox && typeof (opts.sandbox) !== 'object') {
        throw new TypeError('opts.sandbox must be an Object');
    }

    if (typeof (opts.dtrace) !== 'object') {
        throw new TypeError('opts.dtrace (Object) required');
    }

    assert.object(opts.log, 'opts.log');
    assert.optionalObject(opts.trace, 'opts.trace');

    if (typeof (opts.runner.do_fork) === 'undefined') {
        opts.runner.do_fork = true;
    }

    var runner = opts.runner;
    var job = opts.job;
    var trace = opts.trace;
    var log = opts.log;
    var backend = opts.backend;
    var sandbox = opts.sandbox || {};
    var dtrace = opts.dtrace;
    var do_fork = opts.runner.do_fork;
    var timeout = null;

    // pointer to child process forked by runTask
    var child = null;
    // Properties of job object which a task should not be allowed to modify:
    var frozen_props = [
        'callback_urls', 'chain', 'chain_results', 'onerror', 'onerror_results',
        'exec_after', 'timeout', 'elapsed', 'uuid', 'workflow_uuid',
        'name', 'execution', 'num_attempts', 'max_attempts', 'initial_delay',
        'max_delay', 'prev_attempt', 'oncancel', 'oncancel_results',
        'workflow', 'created_at', 'started', 'log', 'name', 'runner_id',
        'locks', 'target'
    ];
    // Our job has been canceled while
    // running. If so, we set this to true:
    var canceled = false;
    var failed = false;
    var failedErr = null;
    // In case we aren't forking tasks:
    var taskRunner = null;

    if (!util.isDate(job.exec_after)) {
        job.exec_after = new Date(job.exec_after);
    }

    if (!job.chain) {
        job.chain = [];
    }

    if (!job.chain_results) {
        job.chain_results = [];
    }

    if (!job.callback_urls) {
        job.callback_urls = [];
    }

    if (job.onerror && !job.onerror_results) {
        job.onerror_results = [];
    }

    if (job.oncancel && !job.oncancel_results) {
        job.oncancel_results = [];
    }

    if (job.timeout) {
        timeout = ((job.elapsed) ?
          (job.timeout - job.elapsed) :
          job.timeout) * 1000;
    }


    function _updateJobProperty(uuid, prop, val, meta, callback) {
        if (typeof (meta) === 'function') {
            callback = meta;
            meta = {};
        }

        var lastError = null;

        var attempt = backoff.exponential({
            initialDelay: 1,
            maxDelay: Infinity
        });

        // Retry max-attempts:
        // attempt.failAfter(10);

        attempt.on('backoff', function (number, delay) {
            // Do something when backoff starts, e.g. show to the
            // user the delay before next reconnection attempt.
        });

        // Do something when backoff ends, e.g. retry a failed
        // operation. If it fails again with BackendInternalError, then
        // backoff, otherwise reset the backoff instance.
        attempt.on('ready', function (number, delay) {
            return backend.updateJobProperty(
                uuid,
                prop,
                val,
                meta,
                function (err) {
                    lastError = err;
                    if (err && err.name === 'BackendInternalError') {
                        return attempt.backoff();
                    } else {
                        attempt.reset();
                        return callback(err);
                    }
                });
        });

        // Do something when the maximum number of backoffs is
        // reached.
        attempt.on('fail', function () {
            return callback(lastError);
        });

        return attempt.backoff();
    }

    function onChildUp() {
        if (do_fork && child) {
            child._pid = child.pid;
            runner.childUp(job.uuid, child._pid);
        }
    }

    function onChildExit() {
        if (do_fork && child) {
            runner.childDown(job.uuid, child._pid);
        }
    }


    function saveJob(callback) {
        job.elapsed = (new Date().getTime() - job.started) / 1000;
        // Decide what to do with the Job depending on its execution status:
        if (
          job.execution === 'failed' ||
          job.execution === 'succeeded' ||
          job.execution === 'canceled') {
            log.trace('Finishing job ...');
            return backend.finishJob(job, function (err, job) {
                runner.releaseSlot();
                if (err) {
                    return callback(err);
                }
                return callback(null, job);
            });
        } else if (job.execution === 'queued') {
            log.trace('Re queueing job ...');
            return backend.queueJob(job, function (err, job) {
                runner.releaseSlot();
                if (err) {
                    return callback(err);
                }
                return callback(null, job);
            });
        } else if (job.execution === 'waiting') {
            log.trace('Pausing job ...');
            return backend.pauseJob(job, function (err, job) {
                runner.releaseSlot();
                if (err) {
                    return callback(err);
                }
                return callback(null, job);
            });
        } else if (job.execution === 'retried') {
            log.trace('Retrying job ...');
            return backend.finishJob(job, function (err, job) {
                runner.releaseSlot();
                if (err) {
                    return callback(err);
                }
                return callback('retry', job);
            });
        } else {
            log.error('Unknown job execution status ' + job.execution);
            runner.releaseSlot();
            return callback('Unknown job execution status ' + job.execution);
        }
    }

    function onEnd(err, callback) {
        if (!err && failed) {
            err = failedErr;
        }
        if (err) {
            if (err === 'queue') {
                job.execution = 'queued';
            } else if (err === 'cancel') {
                job.execution = 'canceled';
            } else if (err === 'retry') {
                job.execution = 'retried';
            } else if (err === 'wait') {
                job.execution = 'waiting';
            } else {
                job.execution = 'failed';
            }
        } else {
            job.execution = 'succeeded';
        }
        return saveJob(callback);
    }

    function runTask(task, chain, cb) {
        var task_start = new Date().toISOString();
        // We may have cancel the job due to runner process exit/restart
        // If that's the case, do not fork, just return:
        if (canceled === true && job.execution === 'queued') {
            return cb('queue');
        }

        if (trace) {
            trace.begin(trace.fields.name + '.' + task.name);
        }

        // Task name, task body, start time (time we fire start probe)
        dtrace.addProbe('wf-task-start',
                        'char *',
                        'char *',
                        'int');
        // Task name, result, error, started_at/finished_at,
        // end time (time we fire done probe):
        dtrace.addProbe('wf-task-done',
                        'char *',
                        'char *',
                        'char *',
                        'int',
                        'int',
                        'int');

        dtrace.fire('wf-task-start', function taskProbeStart() {
            var ret = [
                task.name,
                task.body,
                new Date().getTime()
            ];
            return (ret);
        });

        function runTaskCb(msg) {
            // Message may contain one of the 'error', 'cmd', or 'info' members,
            // plus 'result'.
            log.trace({message: msg}, 'child process message');

            if (msg.info) {
                var info = {
                    data: msg.info,
                    date: new Date().toISOString()
                };
                return backend.addInfo(job.uuid, info, function (err) {
                    if (err) {
                        log.error({err: err}, 'Error adding info');
                    }
                });
            }

            if (do_fork) {
                // If we don't have msg.info member, it's safe to tell the
                // child process to exit if it hasn't done yet:
                if (child && child.exitCode === null) {
                    child.kill();
                }
            } else {
                // Once we're done, taskRunner should be reset for next task:
                if (taskRunner) {
                    // Allow tasks which might go haywire, a chance to know the
                    // task should have been complete.
                    taskRunner.markDone();
                }
                taskRunner = null;
            }

            // Save the results into the result chain + backend update.
            var res = {
                result: msg.result,
                error: msg.error,
                name: msg.task_name,
                started_at: task_start,
                finished_at: new Date().toISOString()
            };

            dtrace.fire('wf-task-done', function tastkProbeDone() {
                var ret = [
                    res.name,
                    res.result,
                    res.error,
                    new Date(res.started_at).getTime(),
                    new Date(res.finished_at).getTime(),
                    new Date().getTime()
                ];
                return (ret);
            });

            if (trace) {
                trace.end(trace.fields.name + '.' + task.name);
            }

            // If the task added/updated any property to the job,
            // let's get it
            if (msg.job) {
                Object.keys(msg.job).forEach(function (p) {
                    if (frozen_props.indexOf(p) === -1) {
                        job[p] = msg.job[p];
                    }
                });
            }

            // Prevent backend double JSON encoding issues, just in case:
            if (!util.isArray(job[chain])) {
                return cb(util.format('Job chain is not an array of ' +
                        'results, but has type %s', typeof (job[chain])));
            } else {
                job[chain].push(res);
                return _updateJobProperty(
                  job.uuid,
                  chain,
                  job[chain],
                  function (err) {
                    // If we canceled the job and got a reply from the
                    // running task we want to stop execution ASAP:
                    if (canceled) {
                        if (job.execution === 'queued') {
                            return cb('queue');
                        } else {
                            if (chain !== 'oncancel_results') {
                                return cb('cancel');
                            } else {
                                return cb(null, res.result);
                            }
                        }
                    } else {
                        // Backend error
                        if (err) {
                            return cb(err);
                        } else if (msg.error) {
                            // Task error
                            return cb(msg.error);
                        } else {
                            // All good:
                            return cb(null, res.result);
                        }
                    }
                });
            }
        }

        if (do_fork) {
            try {
                child = fork(__dirname + '/child.js');
            } catch (e) {
                // If we can't fork, log error and re-queue the job execution
                log.error(e, 'Error forking child process');
                return cb('queue');
            }

            // Keep withing try/catch block and prevent wf-runner exiting if
            // child exits due to out of memory
            try {
                onChildUp();
                // Message may contain one of the 'error', 'cmd', or 'info'
                // members, plus 'result'.
                child.on('message', runTaskCb);

                child.on('exit', function (code) {
                    onChildExit();
                });

                return child.send({
                    task: task,
                    job: job,
                    sandbox: sandbox
                });
            } catch (ex) {
                log.error(ex, 'Error from child process');
                onChildExit();
                return cb(ex);
            }
        } else {
            taskRunner = WorkflowTaskRunner({
                task: task,
                job: clone(job),
                sandbox: sandbox
            });
            return taskRunner.runTask(runTaskCb);
        }
    }

    // Run the given chain of tasks
    // Arguments:
    // - chain: the chain of tasks to run.
    // - chain_results: the name of the job property to append current chain
    //   results. For main `chain` it'll be `job.chain_results`; for `onerror`
    //   branch, it'll be `onerror_results` and so far.
    // - callback: f(err)
    function runChain(chain, chain_results, callback) {
        var timeoutId, chain_to_run;

        if (timeout) {
            timeoutId = setTimeout(function () {
                // Execution of everything timed out, have to abort running
                // tasks and run the onerror chain.
                clearTimeout(timeoutId);
                if (do_fork && child) {
                    process.kill(child._pid, 'SIGTERM');
                }
                // If it's already failed, what it's timing out is the 'onerror'
                // chain. We don't wanna run it again.
                if (!failed) {
                    job[chain_results].push({
                        error: 'workflow timeout',
                        result: ''
                    });
                    _updateJobProperty(
                        job.uuid,
                        chain_results,
                        job[chain_results],
                        function (err) {
                            if (err) {
                                return onEnd('backend error', callback);
                            }
                            return onError('workflow timeout', callback);
                        });
                } else {
                    job.onerror_results.push({
                        error: 'workflow timeout',
                        result: ''
                    });
                    _updateJobProperty(
                        job.uuid,
                        chain_results,
                        job.onerror_results,
                        function (err) {
                            if (err) {
                                return onEnd('backend error', callback);
                            }
                            return onEnd('workflow timeout', callback);
                        });
                }
            }, timeout);
        }

        // Job may have been re-queued. If that's the case, we already
        // have results for some tasks: restart from the task right
        // after the one which re-queued the workflow.
        if (job[chain_results].length) {
            chain_to_run = chain.slice(
              job[chain_results].length, chain.length);
        } else {
            chain_to_run = chain;
        }

        var pipeline = chain_to_run.map(function (task) {
            return (function (_, cb) {
                if (task.modules && typeof (task.modules) === 'string') {
                    try {
                        task.modules = JSON.parse(task.modules);
                    } catch (e) {
                        delete task.modules;
                    }
                }
                return runTask(task, chain_results, cb);
            });
        });

        vasync.pipeline({
            funcs: pipeline
        }, function (err, results) {
            log.trace({results: results}, 'Pipeline results');
            // Whatever happened here, we are timeout done.
            if (timeoutId) {
                clearTimeout(timeoutId);
            }

            if (err) {
                // If we are cancelating job, we want to avoid running
                // "onerror" branch
                if (err === 'cancel') {
                    return onCancel(callback);
                } else {
                    return onError(err, callback);
                }
            } else {
                // All tasks run successful. Need to report information so,
                // we rather emit 'end' and delegate into another function
                // (unless we are running the onCancel chain)
                if (canceled) {
                    return onEnd('cancel', callback);
                }
                return onEnd(null, callback);
            }
        });
    }


    function onCancel(callback) {
        canceled = true;
        if (job.oncancel && util.isArray(job.oncancel)) {
            log.trace('Running oncancel');
            return runChain(
                job.oncancel, 'oncancel_results', function (err) {
                    if (err) {
                        log.error({err: err}, 'Error running oncancel chain');
                    }
                    return onEnd('cancel', callback);
                });
        } else {
            return onEnd('cancel', callback);
        }
    }


    function onError(err, callback) {
        // We're already running the onerror chain, do not retry again!
        if (failed) {
            return onEnd(err, callback);
        } else {
            if (err === 'queued') {
                return onEnd('queue', callback);
            } else if (err === 'retry') {
                return onEnd('retry', callback);
            } else if (err === 'wait') {
                return onEnd('wait', callback);
            } else {
                failed = true;
                failedErr = err;
                if (job.onerror && util.isArray(job.onerror)) {
                    return runChain(
                        job.onerror, 'onerror_results', callback);
                } else {
                    return onEnd(err, callback);
                }
            }
        }
    }


    return ({
        timeout: timeout,

        cancel: function cancel(execution, callback) {
            canceled = true;
            if (execution === 'canceled') {
                if (do_fork && child) {
                    child.send({
                        cmd: 'cancel'
                    });
                } else if (taskRunner) {
                    taskRunner.canceled = true;
                }
                job.execution = 'canceled';
            } else if (execution === 'queued') {
                job.execution = 'queued';
            }
            return callback();
        },

        saveJob: saveJob,

        onChildUp: onChildUp,

        onChildExit: onChildExit,

        onEnd: onEnd,

        onError: onError,

        runTask: runTask,

        runChain: runChain,
        // Run the workflow within a timeout which, in turn, will call tasks in
        // chain within their respective timeouts when given:
        // Arguments:
        // - callback: f(err) - Used to send final job results
        run: function run(callback) {
            runner.getSlot();
            // Keep track of time:
            job.started = new Date().getTime();
            runChain(job.chain, 'chain_results', callback);
        }

    });
};
