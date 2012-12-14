// Copyright 2012 Pedro P. Candel <kusorbox@gmail.com>. All rights reserved.
var bunyan = require('bunyan');
var util = require('util');
var vm = require('vm');

var TimeoutError = function (msg) {
    this.name = 'TimeoutError';
    this.message = msg || 'Timeout Error';
};

TimeoutError.prototype = new Error();
TimeoutError.prototype.constructor = TimeoutError;

// Run a single task.
// - opts (Object) required options to run the task:
//   - job (Object) the job current task is part of.
//   - task (Object) the task to run.
//   - sandbox (Object) the sandbox to pass to the VM where the task run
//     in the form:
//     {
//          "any_var": "aValue",
//          "modules": {
//              'module_global_var_name': 'node-module-name'
//          }
//     }
// Will return an object with the following information:
// - job (Object) the job object updated with any modification the task
//   may need to realize. Note this is the only way of communication between
//   tasks.
// - result (String) information about task results.
// - error (String) when an error has happened, descriptive information.
// - cmd (String) next command workflow should run after this task. Right now
//   one of 'run', 'error' or 'queue'.
var WorkflowTaskRunner = module.exports = function (opts) {

    if (typeof (opts) !== 'object') {
        throw new TypeError('opts (Object) required');
    }

    if (typeof (opts.job) !== 'object') {
        throw new TypeError('opts.job (Object) required');
    }

    if (typeof (opts.task) !== 'object') {
        throw new TypeError('opts.task (Object) required');
    }

    var sandbox = {
        setTimeout: global.setTimeout,
        clearTimeout: global.clearTimeout,
        setInterval: global.setInterval,
        clearInterval: global.clearInterval,
        console: global.console
    };

    if (opts.sandbox) {
        if (typeof (opts.sandbox) !== 'object') {
            throw new TypeError('opts.sandbox must be an Object');
        } else {
            Object.keys(opts.sandbox).forEach(function (k) {
                if (k === 'modules') {
                    // Allow tasks to load none or some of the sandbox
                    // modules:
                    if (opts.task.modules) {
                        if (typeof (opts.task.modules) !== 'object') {
                            throw new TypeError(
                                'opts.task.modules must be an Object');
                        }
                        Object.keys(opts.task.modules).forEach(
                            function (mod) {
                            global[mod] = require(opts.task.modules[mod]);
                            sandbox[mod] = global[mod];
                        });
                    } else {
                        Object.keys(opts.sandbox.modules).forEach(
                            function (mod) {
                            global[mod] = require(opts.sandbox.modules[mod]);
                            sandbox[mod] = global[mod];
                        });
                    }
                } else {
                    sandbox[k] = opts.sandbox[k];
                }
            });
        }
    }

    var context = vm.createContext(sandbox);

    var job = opts.job;
    var name = opts.task.name || opts.task.uuid;
    // Number of times to attempt the task
    var retry = opts.task.retry || 1;
    // Timeout for the task, when given
    var timeout = (opts.task.timeout * 1000) || null;
    var body = null;
    // First, wrap into try/catch, since it may be invalid JavaScript:
    try {
        body = vm.runInContext('(' + opts.task.body + ')', context);
    } catch (e) {
        throw new TypeError('opt.task.body (String) must be a Function source');
    }

    // Even if it is valid JavaScript code, we need it to be a function:
    if (typeof (body) !== 'function') {
        throw new TypeError('opt.task.body (String) must be a Function source');
    }

    var fallback;
    try {
        fallback = (!opts.task.fallback) ? null :
          vm.runInContext('(' + opts.task.fallback + ')', context);
    } catch (err) {
        throw new TypeError(
          'opt.task.fallback (String) must be a Function source');
    }

    if (fallback && typeof (fallback) !== 'function') {
        throw new TypeError(
          'opt.task.fallback (String) must be a Function source');
    }

    // Number of already run retries:
    var retries = 0;
    var retryTimedOut = false;
    // Placeholder for timeout identifiers:
    var taskTimeoutId = null;
    var taskFallbackTimeoutId = null;
    // Need to keep a reference to timeout exeception listener
    var timeoutListener = null;

    var taskCallback = null;

    var taskRunner = {
        name: name,
        body: body,
        fallback: fallback,
        timeout: timeout,
        // Received cancelation message from parent?
        canceled: false
    };

    function formatResults(msg) {
        if (!msg.result) {
            msg.result = '';
        }

        if (!msg.error) {
            msg.error = '';
        }

        // GH-82: If we have an error instance, try to provide useful
        // information, like restify.Error does:
        if (msg.error && typeof (msg.error) !== 'string' &&
                !msg.error.statusCode) {
            // NOTE: Intentionally avoiding error.stack, since it will point to
            // this file despite of task.body contents.
            msg.error = {
                name: msg.error.name,
                message: msg.error.message
            };
        }
        if (msg.info) {
            msg.cmd = 'info';
        }

        if (!msg.cmd) {
            if (msg.error === '') {
                msg.cmd = 'run';
            } else {
                msg.cmd = (msg.error === 'queue') ? 'queue' : 'error';
            }
        }

        msg.job = {};
        var p;
        for (p in job) {
            if (p === 'log') {
                continue;
            }
            msg.job[p] = job[p];
        }

        msg.task_name = name;
        return msg;
    }


    // setup a bunyan logger that logs to stderr and info
    function JobStream() {}
    JobStream.prototype.write = function (rec) {
        // We call the callback each time, but this just writes the
        // message to the parent, multiple calls is ok.
        return taskCallback(formatResults({
            info: rec
        }));
    };

    var logStreams = [
        {
            type: 'raw',
            stream: (new JobStream()),
            level: 'trace'
        },
        {
            stream: process.stdout,
            level: 'trace'
        }
    ];

    var log = new bunyan({
        name: name,
        job_uuid: job.uuid,
        streams: logStreams
    });

    job.log = log;

    function clearTaskTimeoutId(tId) {
        if (tId) {
            process.removeListener('uncaughtException', timeoutListener);
            clearTimeout(tId);
            tId = null;
        }
    }

    // A retry may fail either due to a task timeout or just a task failure:
    function onRetryError(err, cb) {
        clearTaskTimeoutId(taskTimeoutId);

        // If job sent a cancelation message, stop here:
        if (taskRunner.canceled) {
            return cb(formatResults({
                error: 'cancel',
                cmd: 'cancel'
            }));
        }

        // If we are not at the latest retry, try again:
        if (retries < retry) {
            return retryTask(cb);
        } else {
            // We are at the latest retry, check if the task has a 'fallback':
            if (fallback) {
                timeoutListener = function (err) {
                    if (err.name === 'TimeoutError') {
                        return cb(formatResults({
                            error: 'task timeout error'
                        }));
                    } else {
                        throw err;
                    }
                };

                // Set the task timeout when given also for fallback:
                if (timeout) {
                    clearTaskTimeoutId(taskFallbackTimeoutId);
                    // Task timeout must be in seconds:
                    taskFallbackTimeoutId = setTimeout(function () {
                        process.on('uncaughtException', timeoutListener);
                        throw new TimeoutError('task timeout error');
                    }, timeout);
                }

                return fallback(err, job, function (error, result) {
                    clearTaskTimeoutId(taskFallbackTimeoutId);
                    // If even the error handler returns an error, we have to
                    // bubble it up:
                    if (error) {
                        return cb(formatResults({
                            error: error
                        }));
                    }
                    // If the 'fallback' handler fixed the error, let's return
                    // success despite of body failure:
                    return cb(formatResults({
                        result: (result) ? result : 'OK'
                    }));
                });
            } else {
                // Latest retry and task 'fallback' is not defined, fail the
                // task save the error and bubble up:
                return cb(formatResults({
                    error: err
                }));
            }
        }
    }

    function retryTask(cb) {
        timeoutListener = function (err) {
            if (err.name === 'TimeoutError') {
                return onRetryError('task timeout error', cb);
            } else {
                throw err;
            }
        };

        // Set the task timeout when given:
        if (timeout) {
            clearTaskTimeoutId(taskTimeoutId);
            // Task timeout must be in seconds:
            taskTimeoutId = setTimeout(function () {
                retryTimedOut = true;
                process.on('uncaughtException', timeoutListener);
                throw new TimeoutError('task timeout error');
            }, timeout);
        }

        return body(job, function (err, res) {
            if (retryTimedOut) {
                retryTimedOut = false;
                return null;
            }

            // Reached callback from task body, clear the taskTimeout first:
            clearTaskTimeoutId(taskTimeoutId);
            // Task invokes callback with an error message:
            if (err) {
                // A task can re-queue a workflow:
                if (err === 'queue') {
                    return cb(formatResults({
                        result: (res) ? res : 'OK',
                        error: 'queue'
                    }));
                }
                return onRetryError(err, cb);
            } else {
                // All good calling the task body, let's save the results and
                // move to next task:
                return cb(formatResults({
                    result: (res) ? res : 'OK'
                }));
            }
        });

    }

    // Run the task. As many retries as required, invoke fallback if
    // necessary. Send back a message with info about results, error,
    // the job object itself to communicate with other tasks and a command
    // to let WorkflowJobRunner how to proceed.
    //
    // - callback - f(message)
    //
    // - message is the same than self.message, and will be composed of the
    //   following members:
    //   - result (String) any information the tasks want to save into the
    //     proper result chain.
    //   - err (String) if there is an error, it'll be here as an string.
    //   - job (Object) the job object itself, without this task's results
    //     appended, so we can pass job properties to the following tasks on the
    //     chain.
    //   - cmd (String) depending on results and the task itself, a clue for the
    //     WorkflowJobRunner to decide what to do next. These values will be one
    //     of 'run', 'error', 'queue'. (In the future we may also implement
    //     'pause' to let the runner set a timeout to continue execution).
    function runTask(callback) {
        taskCallback = callback;
        return retryTask(callback);
    }

    taskRunner.runTask = runTask;
    return taskRunner;
};
