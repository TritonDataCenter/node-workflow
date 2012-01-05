// Copyright 2012 Pedro P. Candel <kusorbox@gmail.com>. All rights reserved.
var util = require('util'),
    events = require('events'),
    vm = require('vm'),
    async = require('async');

var Workflow = module.exports = function(job) {
  events.EventEmitter.call(this);
  var p;
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

  if (!this.timeout) {
    this.timeout = 5 * 60 * 1000;
  }
  
  // Listeners:
  this.on('timeout', function() {
    // Decide what to do on timeout, error.
    // Just call 'onerror' chain here too?. Cause I'd say onerror chain
    // should run within the same timeout ...
    // PENDING!
  });

  this.on('error', function(err) {
    // Called when we cannot recover from a chain failure. Will run 'onerror'
    // chain when present.
    // PENDING:
    // - onerror chain call.
    // - onerror chain results.
  });

  // Sandbox we'll pass to tasks:
  this.sandbox = {
    setTimeout: global.setTimeout,
    clearTimeout: global.clearTimeout,
    setInterval: global.setInterval,
    clearInterval: global.clearInterval
  }
};

util.inherits(Workflow, events.EventEmitter);

// Run the workflow within a timeout which, in turn, will call tasks in chain
// within their respective timeouts when given:
// PENDING:
// - Need to verify that exec_after is smaller than current time, otherwise the
//   job execution should be delayed.
Workflow.prototype.run = function() {
  var self = this,
  timeoutId = setTimeout(function() {
    // Execution of everything timed out, have to abort running tasks and run
    // the onerror chain.
    self.emit('timeout');
  }, self.timeout);
  
  async.forEachSeries(self.chain, self.runTask, function(err) {
    // Whatever happened here, we are timeout done.
    // PENDING: Do we want to run 'onerror' chain within a timeout, same one?
    clearTimeout(timeoutId);
    if (err) {
      self.emit('error', err);
    } else {
      // All tasks run successful. Need to report information so, we rather
      // emit 'end' and delegate into another function
      self.emit('end');
    }
  });
};


Workflow.prototype.runTask = function(task, cb) {
  var self = this,
      retries = 0,
      taskTimeout = null,
      taskTimeoutId = null,
      retryTimedOut = false,
      // TODO: We may want to pass something else to our sandbox here:
      fun = vm.runInNewContext('(' + task.body + ')', self.sandbox),
      errfun = (task.onerror) ?
               vm.runInNewContext('(' + task.onerror + ')', self.sandbox):
               null,
      retryTask = function() {
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
            // All good calling the task body, let's save the results and move to
            // next task:
            if (!retryTimedOut) {
              // Sometimes this may be called after we timed out the current
              // call to retryTask and we've already moved into the next one.
              // On such case, we will not update results at this pass.
              self.results.push({
                error: '',
                result: (res) ? res : 'OK'
              });
            }
            return cb();
          }
        });
      },
      // A retry may fail either due to a task timeout or just a task failure:
      onRetryError = function(err) {
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
                self.results.push({
                  error: error,
                  result: ''
                });
                return cb(error);
              }
              // If the 'onerror' handler fixed the error, let's return
              // success despite of body failure:
              self.results.push({
                error: '',
                result: (result) ? result : 'OK'
              });
              return cb();
            });
          } else {
            // Latest retry and task 'onerror' is not defined, fail the task
            // save the error and bubble up:
            self.results.push({
              error: err,
              result: ''
            });
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
