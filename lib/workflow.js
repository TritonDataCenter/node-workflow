// Copyright 2012 Pedro P. Candel <kusorbox@gmail.com>. All rights reserved.
var util = require('util'),
    events = require('events'),
    vm = require('vm');

// TODO:
// - We may want to save a timestamp with each task results.
// - Implement tasks ability to re-queue a workflow 'callback('queue')'.
var Workflow = module.exports = function(job, sandbox) {
  events.EventEmitter.call(this);
  var p, the_sandbox;
  this.job = job;
  // Set all properties from job object
  for (p in job) {
    if (p === 'exec_after') {
      this[p] = new Date(job[p]);
    } else {
      this[p] = job[p];
    }
  }

  if (!this.chain) {
    this.chain = [];
  }

  if (!this.job.chain_results) {
    this.job.chain_results = [];
  }

  if (typeof this.job.chain_results === 'string') {
    this.job.chain_results = JSON.parse(this.job.chain_results);
  }

  if (this.onerror && !this.job.onerror_results) {
    this.job.onerror_results = [];
  }

  if (this.job.onerror_results &&
      typeof this.job.onerror_results === 'string'
  ) {
    this.job.onerror_results = JSON.parse(this.job.onerror_results);
  }

  if (!this.timeout) {
    this.timeout = 60 * 60 * 1000;
  }

  this.on('error', function(err, callback) {
    var self = this;
    // We're already running the onerror chain, do not retry again!
    if (self.failed) {
      self.emit('end', err, callback);
    } else {
      self.failed = true;
      if (self.onerror && self.onerror.length !== 0) {
        self.runChain(self.onerror, self.job.onerror_results, callback);
      } else {
        self.emit('end', err, callback);
      }
    }
  });

  this.on('end', function(err, callback) {
    var self = this;
    // Note this may be 'queue' when we implement task ability to
    // re-queue a workflow.
    if (err) {
      self.failure = err;
      self.job.execution = 'failed';
    } else {
      self.job.execution = 'succeeded';
    }
    return callback(err);
  });

  // Sandbox we'll pass to tasks:
  the_sandbox = {
    setTimeout: global.setTimeout,
    clearTimeout: global.clearTimeout,
    setInterval: global.setInterval,
    clearInterval: global.clearInterval
  };

  if (sandbox) {
    Object.keys(sandbox).forEach(function(mod) {
      the_sandbox[mod] = require(sandbox[mod]);
    });
  }

  this.sandbox = the_sandbox;
};

util.inherits(Workflow, events.EventEmitter);

// Run the workflow within a timeout which, in turn, will call tasks in chain
// within their respective timeouts when given:
// PENDING:
// - Need to verify that exec_after is smaller than current time, otherwise the
//   job execution should be delayed.
// Arguments:
// - notifier: f(job) - Used to send tasks completion messages
// - callback: f(err) - Used to send final job results
Workflow.prototype.run = function(notifier, callback) {
  var self = this;
  self.notifier = notifier;
  self.runChain(self.chain, self.job.chain_results, callback);
};

// Run the given chain of tasks
// Arguments:
// - chain: the chain of tasks to run.
// - chain_results: an array to push every task results into. For main `chain`
//   it'll be `job.chain_results`, for `onerror` branch, it'll be
//   `onerror_results` and so far.
// - callback: f(err)
Workflow.prototype.runChain = function(chain, chain_results, callback) {
  var self = this,
  task, err,
  timeoutId = setTimeout(function() {
    // Execution of everything timed out, have to abort running tasks and run
    // the onerror chain.
    clearTimeout(timeoutId);
    // May want to ignore tasks results once we timed out the whole workflow
    self.timedOut = true;
    // If it's already failed, what it's timing out is the 'onerror' chain.
    // We don't wanna run it again.
    if (!self.failed) {
      chain_results.push({
        error: 'workflow timeout',
        result: ''
      });
      self.emit('error', 'workflow timeout', callback);
    } else {
      self.job.onerror_results.push({
        error: 'workflow timeout',
        result: ''
      });
      self.emit('end', 'workflow timeout', callback);
    }
  }, self.timeout),
  cb = function(error) {
    // Whatever happened here, we are timeout done.
    clearTimeout(timeoutId);
    if (error) {
      err = error;
      self.emit('error', error, callback);
    } else {
      // All tasks run successful. Need to report information so, we rather
      // emit 'end' and delegate into another function
      self.emit('end', null, callback);
    }
  };

  for (task = 0; task < chain.length; task += 1) {
    if (err) {
      break;
    }
    self.runTask(chain[task], chain_results, cb);
  }

};

// Run the given task. `cb` will call `async.forEachSeries` callback,
// (with error argument when proceed), and move into the next task.
Workflow.prototype.runTask = function(task, chain_results, cb) {
  var self = this,
      retries = 0,
      taskTimeout = null,
      taskTimeoutId = null,
      retryTimedOut = false,
      // TODO: We may want to pass something else to our sandbox here:
      fun = vm.runInNewContext('(' + task.body + ')', self.sandbox),
      errfun = (task.fallback) ?
               vm.runInNewContext('(' + task.fallback + ')', self.sandbox) :
               null,
      retryTask = function() {
        // Do not attempt to run a task if the workflow is already timed out.
        if (self.timedOut) {
          return;
        }
        retries += 1;
        // Set the task timeout when given:
        if (task.timeout) {
          if (taskTimeoutId) {
            clearTimeout(taskTimeoutId);
            taskTimeoutId = null;
          }
          // Task timeout must be in seconds:
          taskTimeout = task.timeout * 1000;
          taskTimeoutId = setTimeout(function() {
            retryTimedOut = true;
            return onRetryError('timeout error');
          }, taskTimeout);
        }

        fun(self.job, function(err, res) {
          // Reached callback from task body, clear the taskTimeout first:
          if (taskTimeoutId) {
            clearTimeout(taskTimeoutId);
          }
          // Task invokes callback with an error message:
          if (err) {
            onRetryError(err);
          } else {
            // All good calling the task body, let's save the results and move
            // to next task:
            if (!retryTimedOut) {
              // Sometimes this may be called after we timed out the current
              // call to retryTask and we've already moved into the next one.
              // On such case, we will not update results at this pass.
              chain_results.push({
                error: '',
                result: (res) ? res : 'OK'
              });
              self.notifier(self.job);
            }
            return cb();
          }
        });
      },
      // A retry may fail either due to a task timeout or just a task failure:
      onRetryError = function(err) {
        // Do not attempt to run a task if the workflow is already timed out.
        if (self.timedOut) {
          return;
        }
        if (taskTimeoutId) {
          clearTimeout(taskTimeoutId);
          taskTimeoutId = null;
        }
        // If we are not at the latest retry, try again:
        if (retries < task.retry) {
          retryTask();
        } else {
          // We are at the latest retry, check if the task has an 'onerror':
          if (errfun) {
            errfun(err, self.job, function(error, result) {
              // If even the error handler returns an error, we have to
              // bubble it up:
              if (error) {
                chain_results.push({
                  error: error,
                  result: ''
                });
                self.notifier(self.job);
                return cb(error);
              }
              // If the 'fallback' handler fixed the error, let's return
              // success despite of body failure:
              chain_results.push({
                error: '',
                result: (result) ? result : 'OK'
              });
              self.notifier(self.job);
              return cb();
            });
          } else {
            // Latest retry and task 'fallback' is not defined, fail the task
            // save the error and bubble up:
            chain_results.push({
              error: err,
              result: ''
            });
            self.notifier(self.job);
            return cb(err);
          }
        }
      };

  if (!task.retry) {
    task.retry = 1;
  }

  // Attempt as many retries as we've been told:
  retryTask();
};
