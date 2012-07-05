// Copyright 2012 Pedro P. Candel <kusorbox@gmail.com>. All rights reserved.
var util = require('util'),
    fork = require('child_process').fork,
    async = require('async'),
    WorkflowTaskRunner = require('./task-runner');

// Run the given job. Optionally, can pass sandbox object for the 'task' VM
// and enable trace to retrieve task trace information.
// - opts (Object) with the following members:
//
// - runner (Object) insteance of the runner running this job. Required to
//   notify the runner about child processes spawned/finished. Required.
// - backend (Object) instance of the backend used. Required.
// - job (Object) the job to run. Required.
// - sandbox (Object) VM's sandbox for task (see WorkflowTaskRunner). Optional.
// - trace (Boolean) retrieve trace information from tasks. Optional.
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

    this.runner = opts.runner;
    this.job = opts.job;
    this.backend = opts.backend;
    this.sandbox = opts.sandbox || {};
    this.log = opts.runner.log.child({job_uuid: opts.job.uuid}, true);

    if (!util.isDate(this.job.exec_after)) {
        this.job.exec_after = new Date(this.job.exec_after);
    }

    if (!this.job.chain) {
        this.job.chain = [];
    }

    if (!this.job.chain_results) {
        this.job.chain_results = [];
    }

    if (this.job.onerror && !this.job.onerror_results) {
        this.job.onerror_results = [];
    }

    this.timeout = null;
    if (this.job.timeout) {
        this.timeout = ((this.job.elapsed) ?
          (this.job.timeout - this.job.elapsed) :
          this.job.timeout) * 1000;
    }
    // Did we consumed the job's total timeout?
    this.jobTimedOut = false;
    // Need to keep elapsed time in msecs
    this.started = null;
    // pointer to child process forked by runTask
    this.child = null;
    // Properties of job object which a task should not be allowed to modify:
    this.frozen_props = [
        'chain', 'chain_results', 'onerror', 'onerror_results',
        'exec_after', 'timeout', 'elapsed', 'uuid', 'workflow_uuid',
        'name', 'execution'
    ];
    // Our job has been canceled while
    // running. If so, we set this to true:
    this.canceled = false;

};


// Run the workflow within a timeout which, in turn, will call tasks in chain
// within their respective timeouts when given:
// Arguments:
// - callback: f(err) - Used to send final job results
WorkflowJobRunner.prototype.run = function (callback) {
    var self = this;
    self.runner.getSlot();
    // Keep track of time:
    self.started = new Date().getTime();
    self.runChain(self.job.chain, 'chain_results', callback);
};


WorkflowJobRunner.prototype.onEnd = function (err, callback) {
    var self = this;
    if (err) {
        self.failure = err;
        if (err === 'queue') {
            self.job.execution = 'queued';
        } else if (err === 'cancel') {
            self.job.execution = 'canceled';
        } else {
            self.job.execution = 'failed';
        }
    } else {
        self.job.execution = 'succeeded';
    }
    return self.saveJob(callback);
};

WorkflowJobRunner.prototype.onError = function (err, callback) {
    var self = this;
    // We're already running the onerror chain, do not retry again!
    if (self.failed) {
        return self.onEnd(err, callback);
    } else {
        if (err === 'queued') {
            return self.onEnd('queue', callback);
        } else {
            self.failed = true;
            if (self.job.onerror && util.isArray(self.job.onerror)) {
                return self.runChain(
                    self.job.onerror, 'onerror_results', callback);
            } else {
                return self.onEnd(err, callback);
            }
        }
    }
};

// Run the given chain of tasks
// Arguments:
// - chain: the chain of tasks to run.
// - chain_results: the name of the job property to append current chain
//   results. For main `chain` it'll be `job.chain_results`; for `onerror`
//   branch, it'll be `onerror_results` and so far.
// - callback: f(err)
WorkflowJobRunner.prototype.runChain = function (
    chain,
    chain_results,
    callback)
{
    var self = this, timeoutId, chain_to_run;

    if (self.timeout) {
        timeoutId = setTimeout(function () {
            // Execution of everything timed out, have to abort running tasks
            // and run the onerror chain.
            clearTimeout(timeoutId);
            if (self.child) {
                process.kill(self.child._pid, 'SIGTERM');
            }
            // If it's already failed, what it's timing out is the 'onerror'
            // chain. We don't wanna run it again.
            if (!self.failed) {
                self.job[chain_results].push({
                    error: 'workflow timeout',
                    result: ''
                });
                self.backend.updateJobProperty(
                  self.job.uuid,
                  chain_results,
                  self.job[chain_results],
                  function (err) {
                    if (err) {
                        return self.onEnd('backend error', callback);
                    }
                    return self.onError('workflow timeout', callback);
                  });
            } else {
                self.job.onerror_results.push({
                    error: 'workflow timeout',
                    result: ''
                });
                self.backend.updateJobProperty(
                  self.job.uuid,
                  chain_results,
                  self.job.onerror_results,
                  function (err) {
                    if (err) {
                        return self.onEnd('backend error', callback);
                    }
                    return self.onEnd('workflow timeout', callback);
                  });
            }
        }, self.timeout);
    }

    if (self.job[chain_results].length) {
        chain_to_run = chain.slice(
          self.job[chain_results].length, chain.length);
    } else {
        chain_to_run = chain;
    }

    async.forEachSeries(chain_to_run, function (task, async_cb) {
        // Job may have been re-queued. If that's the case, we already have
        // results for some tasks: restart from the task right after the one
        // which re-queued the workflow.
        self.runTask(task, chain_results, async_cb);
    }, function (err) {
        // Whatever happened here, we are timeout done.
        if (timeoutId) {
            clearTimeout(timeoutId);
        }

        if (err) {
            // If we are cancelating job, we want to avoid running
            // "onerror" branch
            if (err === 'cancel') {
                return self.onEnd('cancel', callback);
            } else {
                return self.onError(err, callback);
            }
        } else {
            // All tasks run successful. Need to report information so,
            // we rather emit 'end' and delegate into another function
            return self.onEnd(null, callback);
        }
    });
};


WorkflowJobRunner.prototype.runTask = function (task, chain, cb) {
    var self = this,
        task_start = new Date().toISOString();
    // We may have cancel the job due to runner process exit/restart
    // If that's the case, do not fork, just return:
    if (self.canceled === true && self.job.execution === 'queued') {
        return cb('queue');
    }

    try {
        self.child = fork(__dirname + '/child.js');
    } catch (e) {
        // If we cannot fork, log exception and re-queue the job execution
        self.log.error({err: e}, 'Error forking child process');
        return cb('queue');
    }

    // Keep withing try/catch block and prevent wf-runner exiting if
    // child exits due to out of memory
    try {

        self.onChildUp();
        // Message may contain either only 'error' member, or also 'cmd',
        // 'result' and 'trace'.
        self.child.on('message', function (msg) {
            if (self.log.debug()) {
                self.log.debug({
                    msg: msg
                }, 'child process message');
            }
            // Save the results into the result chain + update on the backend.
            var res = {
                result: msg.result,
                error: msg.error,
                name: msg.task_name,
                started_at: task_start,
                finished_at: new Date().toISOString()
            };
            // If the task added/updated any property to the job, let's get it
            if (msg.job) {
                Object.keys(msg.job).forEach(function (p) {
                    if (self.frozen_props.indexOf(p) === -1) {
                        self.job[p] = msg.job[p];
                    }
                });
            }

            if (msg.trace) {
                res.trace = msg.trace;
            }

            // Prevent backend double JSON encoding issues, just in case:
            if (!util.isArray(self.job[chain])) {
                return cb(util.format('Job chain is not an array of results,' +
                            ' but has type %s', typeof (self.job[chain])));
            } else {
                self.job[chain].push(res);
                return self.backend.updateJobProperty(
                  self.job.uuid,
                  chain,
                  self.job[chain],
                  function (err) {
                    // If we canceled the job and got a reply from the running
                    // task we want to stop execution ASAP:
                    if (self.canceled) {
                        if (self.job.execution === 'queued') {
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
                            return cb(null);
                        }
                    }
                  });
            }

        });

        self.child.on('exit', function (code) {
            self.onChildExit();
        });

        return self.child.send({
            task: task,
            job: self.job,
            sandbox: self.sandbox,
            trace: self.log.trace()
        });

    } catch (ex) {
        self.log.error({err: ex}, 'Error from child process');
        self.onChildExit();
        return cb(ex);
    }
};

WorkflowJobRunner.prototype.onChildUp = function () {
    var self = this;
    if (self.child) {
        self.child._pid = self.child.pid;
        self.runner.childUp(self.job.uuid, self.child._pid);
    }
};

WorkflowJobRunner.prototype.onChildExit = function () {
    var self = this;
    if (self.child) {
        self.runner.childDown(self.job.uuid, self.child._pid);
        self.child = null;
    }
};

// - callback - f(err)
WorkflowJobRunner.prototype.saveJob = function (callback) {
    var self = this;
    self.job.elapsed = (new Date().getTime() - self.started) / 1000;
    // Decide what to do with the Job depending on its execution status:
    if (
      self.job.execution === 'failed' ||
      self.job.execution === 'succeeded' ||
      self.job.execution === 'canceled') {
        self.log.trace('Finishing job ...');
        return self.backend.finishJob(self.job, function (err, job) {
            self.runner.releaseSlot();
            if (err) {
                return callback(err);
            }
            return callback(null, job);
        });
    } else if (self.job.execution === 'queued') {
        self.log.trace('Re queueing job ...');
        return self.backend.queueJob(self.job, function (err, job) {
            self.runner.releaseSlot();
            if (err) {
                return callback(err);
            }
            return callback(null, job);
        });
    } else {
        self.log.error('Unknown job execution status ' + self.job.execution);
        self.runner.releaseSlot();
        return callback('Unknown job execution status ' + self.job.execution);
    }
};


// - execution - String, the usual execution values.
// - callback - f(err)
WorkflowJobRunner.prototype.cancel = function (execution, callback) {
    var self = this;
    self.canceled = true;
    if (execution === 'canceled') {
        if (self.child) {
            self.child.send({
                cmd: 'cancel'
            });
        }
        self.job.execution = 'canceled';
    } else if (execution === 'queued') {
        self.job.execution = 'queued';
    }
    callback();
};
