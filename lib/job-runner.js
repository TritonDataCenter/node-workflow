// Copyright 2013 Pedro P. Candel <kusorbox@gmail.com>. All rights reserved.
var util = require('util');
var fork = require('child_process').fork;
var vasync = require('vasync');
var WorkflowTaskRunner = require('./task-runner');
var backoff = require('backoff');

// Run the given job. Optionally, can pass sandbox object for the 'task' VM
// - opts (Object) with the following members:
//
// - runner (Object) insteance of the runner running this job. Required to
//   notify the runner about child processes spawned/finished. Required.
// - backend (Object) instance of the backend used. Required.
// - job (Object) the job to run. Required.
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

    var runner = opts.runner;
    var job = opts.job;
    var backend = opts.backend;
    var sandbox = opts.sandbox || {};
    var log = opts.runner.log.child({job_uuid: opts.job.uuid}, true);
    var dtrace = opts.dtrace;
    var timeout = null;

    // pointer to child process forked by runTask
    var child = null;
    // Properties of job object which a task should not be allowed to modify:
    var frozen_props = [
        'chain', 'chain_results', 'onerror', 'onerror_results',
        'exec_after', 'timeout', 'elapsed', 'uuid', 'workflow_uuid',
        'name', 'execution', 'num_attempts', 'max_attempts', 'initial_delay',
        'max_delay', 'prev_attempt'
    ];
    // Our job has been canceled while
    // running. If so, we set this to true:
    var canceled = false;
    var failed = false;

    if (!util.isDate(job.exec_after)) {
        job.exec_after = new Date(job.exec_after);
    }

    if (!job.chain) {
        job.chain = [];
    }

    if (!job.chain_results) {
        job.chain_results = [];
    }

    if (job.onerror && !job.onerror_results) {
        job.onerror_results = [];
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
        if (child) {
            child._pid = child.pid;
            runner.childUp(job.uuid, child._pid);
        }
    }

    function onChildExit() {
        if (child) {
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

        dtrace.fire('wf-task-start', function taskProbeStart() {
            var ret = [
                task.name,
                task.body,
                new Date().getTime()
            ];
            return (ret);
        });

        try {
            child = fork(__dirname + '/child.js');
        } catch (e) {
            // If we cannot fork, log exception and re-queue the job execution
            log.error(e, 'Error forking child process');
            return cb('queue');
        }

        // Keep withing try/catch block and prevent wf-runner exiting if
        // child exits due to out of memory
        try {

            onChildUp();
            // Message may contain one of the 'error', 'cmd', or 'info' members,
            // plus 'result'.
            child.on('message', function (msg) {
                log.trace({message: msg}, 'child process message');

                if (msg.info) {
                    var info = {
                        data: msg.info,
                        date: new Date().toISOString()
                    };
                    return backend.addInfo(job.uuid, info,
                        function (err) {
                        if (err) {
                            log.error({err: err}, 'Error adding info');
                        }
                    });
                }

                // If we don't have msg.info member, it's safe to tell the child
                // process to exit if it hasn't done yet:
                if (child && child.exitCode === null) {
                    child.kill();
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
                                return cb('cancel');
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

            });

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
                if (child) {
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

        if (job[chain_results].length) {
            chain_to_run = chain.slice(
              job[chain_results].length, chain.length);
        } else {
            chain_to_run = chain;
        }

        var pipeline = chain_to_run.map(function (task) {
            return (function (_, cb) {
                // Job may have been re-queued. If that's the case, we already
                // have results for some tasks: restart from the task right
                // after the one which re-queued the workflow.
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
                    return onEnd('cancel', callback);
                } else {
                    return onError(err, callback);
                }
            } else {
                // All tasks run successful. Need to report information so,
                // we rather emit 'end' and delegate into another function
                return onEnd(null, callback);
            }
        });
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
                if (child) {
                    child.send({
                        cmd: 'cancel'
                    });
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
