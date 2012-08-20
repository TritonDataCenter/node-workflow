// Copyright 2012 Pedro P. Candel <kusorbox@gmail.com>. All rights reserved.
var bunyan = require('bunyan'),
    util = require('util'),
    vm = require('vm');

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
//   - trace (Boolean) when set to true, it will return information
//     about the methods called while running the task, and the number of
//     retries for task.body.
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
    },
    context;

    if (opts.sandbox) {
        if (typeof (opts.sandbox) !== 'object') {
            throw new TypeError('opts.sandbox must be an Object');
        } else {
            Object.keys(opts.sandbox).forEach(function (k) {
                if (k === 'modules') {
                    Object.keys(opts.sandbox.modules).forEach(function (mod) {
                        sandbox[mod] = require(opts.sandbox.modules[mod]);
                    });
                } else {
                    sandbox[k] = opts.sandbox[k];
                }
            });
        }
    }

    context = vm.createContext(sandbox);

    this.job = opts.job;
    this.uuid = opts.task.uuid;
    this.name = opts.task.name || opts.task.uuid;
    // Number of times to attempt the task
    this.retry = opts.task.retry || 1;
    // Timeout for the task, when given
    this.timeout = (opts.task.timeout * 1000) || null;
    // First, wrap into try/catch, since it may be invalid JavaScript:
    try {
        this.body = vm.runInContext('(' + opts.task.body + ')', context);
    } catch (e) {
        throw new TypeError('opt.task.body (String) must be a Function source');
    }

    // Even if it is valid JavaScript code, we need it to be a function:
    if (typeof (this.body) !== 'function') {
        throw new TypeError('opt.task.body (String) must be a Function source');
    }

    try {
        this.fallback = (!opts.task.fallback) ? null :
          vm.runInContext('(' + opts.task.fallback + ')', context);
    } catch (err) {
        throw new TypeError(
          'opt.task.fallback (String) must be a Function source');
    }

    if (this.fallback && typeof (this.fallback) !== 'function') {
        throw new TypeError(
          'opt.task.fallback (String) must be a Function source');
    }

    // Number of already run retries:
    this.retries = 0;
    this.retryTimedOut = false;
    // Placeholder for timeout identifiers:
    this.taskTimeoutId = null;
    this.taskFallbackTimeoutId = null;
    // Need to keep a reference to timeout exeception listener
    this.timeoutListener = null;
    // When trace is enabled, we need to retrieve this info:
    this.stack = {
        retries: 0,
        methods: []
    };
    // Return trace information?
    this.trace = opts.trace || false;
    // Received cancelation message from parent?
    this.canceled = false;
};


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
//     WorkflowJobRunner to decide what to do next. These values will be one of
//     'run', 'error', 'queue'. (In the future we may also implement 'pause' to
//     let the runner set a timeout to continue execution).
WorkflowTaskRunner.prototype.runTask = function (callback) {
    var self = this;

    self.stack.methods.push('runTask');

    // Go for it!
    return self.retryTask(callback);
};


WorkflowTaskRunner.prototype.retryTask = function (cb) {
    var self = this,
        logStreams;

    self.timeoutListener = function (err) {
        if (err.name === 'TimeoutError') {
            return self.onRetryError('task timeout error', cb);
        } else {
            throw err;
        }
    };
    self.stack.retries = self.retries += 1;
    self.stack.methods.push('retryTask');

    // Set the task timeout when given:
    if (self.timeout) {
        self.clearTaskTimeoutId();
        // Task timeout must be in seconds:
        self.taskTimeoutId = setTimeout(function () {
            self.stack.methods.push('taskTimeoutId');
            self.retryTimedOut = true;
            process.on('uncaughtException', self.timeoutListener);
            throw new TimeoutError('task timeout error');
        }, self.timeout);
    }

    // setup a bunyan logger that logs to stderr and info
    function JobStream() {}
    JobStream.prototype.write = function (rec) {
        // We call the callback each time, but this just writes the
        // message to the parent, multiple calls is ok.
        return cb(self.formatResults({
            info: rec
        }));
    };

    logStreams = [
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

    self.job.log = new bunyan({
        name: self.name,
        job_uuid: self.job.uuid,
        streams: logStreams
    });

    self.body(self.job, function (err, res) {
        if (self.retryTimedOut) {
            self.retryTimedOut = false;
            return null;
        }

        self.stack.methods.push('task.body callback');

        // Reached callback from task body, clear the taskTimeout first:
        self.clearTaskTimeoutId();
        // Task invokes callback with an error message:
        if (err) {
            // A task can re-queue a workflow:
            if (err === 'queue') {
                return cb(self.formatResults({
                    result: (res) ? res : 'OK',
                    error: 'queue'
                }));
            }
            return self.onRetryError(err, cb);
        } else {
            // All good calling the task body, let's save the results and move
            // to next task:
            return cb(self.formatResults({
                result: (res) ? res : 'OK'
            }));
        }
    });

};

// A retry may fail either due to a task timeout or just a task failure:
WorkflowTaskRunner.prototype.onRetryError = function (err, cb) {
    var self = this;

    self.stack.methods.push('onRetryError');
    self.clearTaskTimeoutId();

    // If job sent a cancelation message, stop here:
    if (self.canceled) {
        return cb(self.formatResults({
            error: 'cancel',
            cmd: 'cancel'
        }));
    }

    // If we are not at the latest retry, try again:
    if (self.retries < self.retry) {
        return self.retryTask(cb);
    } else {
        // We are at the latest retry, check if the task has a 'fallback':
        if (self.fallback) {
            self.timeoutListener = function (err) {
                if (err.name === 'TimeoutError') {
                    return cb(self.formatResults({
                        error: 'task timeout error'
                    }));
                } else {
                    throw err;
                }
            };

            // Set the task timeout when given also for fallback:
            if (self.timeout) {
                if (self.taskFallbackTimeoutId) {
                    process.removeListener('uncaughtException',
                        self.timeoutListener);
                    clearTimeout(self.taskFallbackTimeoutId);
                    self.taskFallbackTimeoutId = null;
                }
                // Task timeout must be in seconds:
                self.taskFallbackTimeoutId = setTimeout(function () {
                    self.stack.methods.push('taskFallbackTimeoutId');
                    process.on('uncaughtException', self.timeoutListener);
                    throw new TimeoutError('task timeout error');
                }, self.timeout);
            }

            return self.fallback(err, self.job, function (error, result) {
                if (self.taskFallbackTimeoutId) {
                    process.removeListener('uncaughtException',
                      self.timeoutListener);
                    clearTimeout(self.taskFallbackTimeoutId);
                    self.taskFallbackTimeoutId = null;
                }

                self.stack.methods.push('task.fallback callback');

                // If even the error handler returns an error, we have to
                // bubble it up:
                if (error) {
                    return cb(self.formatResults({
                        error: error
                    }));
                }
                // If the 'fallback' handler fixed the error, let's return
                // success despite of body failure:
                return cb(self.formatResults({
                    result: (result) ? result : 'OK'
                }));
            });
        } else {
            // Latest retry and task 'fallback' is not defined, fail the task
            // save the error and bubble up:
            return cb(self.formatResults({
                error: err
            }));
        }
    }
};


WorkflowTaskRunner.prototype.formatResults = function (msg) {
    var self = this;

    if (!msg.result) {
        msg.result = '';
    }

    if (!msg.error) {
        msg.error = '';
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

    msg.job = self.job;
    if (msg.job.log) {
        delete msg.job.log;
    }
    if (self.trace) {
        msg.trace = self.stack;
    }
    msg.task_name = self.name;
    return msg;
};


WorkflowTaskRunner.prototype.clearTaskTimeoutId = function () {
    var self = this;
    if (self.taskTimeoutId) {
        process.removeListener('uncaughtException', self.timeoutListener);
        clearTimeout(self.taskTimeoutId);
        self.taskTimeoutId = null;
    }
};
