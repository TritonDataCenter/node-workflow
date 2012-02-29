// Copyright 2012 Pedro P. Candel <kusorbox@gmail.com> All rights reserved.
var util = require('util'),
    uuid = require('node-uuid'),
    fs = require('fs'),
    path = require('path'),
    async = require('async'),
    Logger = require('bunyan'),
    WorkflowJobRunner = require('./job-runner');

// - opts - configuration options:
//    - identifier: Unique identifier for this runner.
//    - forks: Max number of child processes to fork at the same time.
//    - run_interval: Check for new jobs every 'run_interval' seconds.
//                    (By default, every 2 minutes).
//    - sandbox: Collection of node modules to pass to the sandboxed tasks
//               execution. Object with the form:
//               {
//                  'module_global_var_name': 'node-module-name'
//               }
//               By default, only the global timeouts are passed to the tasks
//               sandbox.
var WorkflowRunner = module.exports = function (opts) {
  if (typeof (opts) !== 'object') {
    throw new TypeError('opts (Object) required');
  }

  if (typeof (opts.backend) !== 'object') {
    throw new TypeError('opts.backend (Object) required');
  }

  if (typeof (opts.runner) !== 'object') {
    opts.runner = {};
  }

  var Backend = require(opts.backend.module);
  this.backend = new Backend(opts.backend.opts);
  this.identifier = opts.runner.identifier || null;
  this.forks = opts.runner.forks || 10;
  this.run_interval = (opts.runner.run_interval || 2) * 1000;
  this.interval = null;
  this.shutting_down = false;
  this.child_processes = [];
  this.sandbox = opts.runner.sandbox || {};
  this.trace = opts.runner.trace || false;

  if (!opts.logger) {
    opts.logger = {};
  }

  opts.logger.name = 'workflow-runner';
  opts.logger.serializers = {
    err: Logger.stdSerializers.err
  };

  opts.logger.streams = opts.logger.streams || [ {
    level: 'info',
    stream: process.stdout
  }];

  opts.logger.runner_uuid = this.identifier;

  this.log = new Logger(opts.logger);
};

WorkflowRunner.prototype.init = function (callback) {
  var self = this,
      series = [];
  // Using async.series we can kind of write this sequentially and run some
  // methods only when needed:

  // 1) We need an unique identifier for this runner, also something
  //    human friendly to identify the runner from a bunch of them would be
  //    helpful.
  if (!self.identifier) {
    series.push(function (cb) {
      self.getIdentifier(function (err, uuid) {
        if (err) {
          return cb(err);
        }
        self.identifier = uuid;
        // Not that we really care about the return value here, anyway:
        return cb(null, uuid);
      });
    });
  }
  // The runner will register itself on the backend, with its unique id.
  series.push(function (cb) {
    self.backend.registerRunner(self.identifier, function (err) {
      if (err) {
        return cb(err);
      }
      return cb(null, null);
    });
  });
  // 2) On init, the runner will check for any job flagged as running with
  //    the runner identifier. This means a previous failure so, first thing
  //    will be take care of such failure.
  //    NOTE: It would be desirable to "hide" the next queued Job to the
  //    other runners when we fetch it for the current one.
  self.backend.init(function () {
    async.series(series, function (err, results) {
      // Note we don't care at all about the results.
      if (err) {
        return callback(err);
      }
      return callback();
    });
  });
};

// Wait for children to finish, do not began any other child process.
// Call callback on done.
WorkflowRunner.prototype.quit = function (callback) {
  var self = this;
  self.shutting_down = true;
  clearInterval(self.interval);
  if (Object.keys(self.child_processes).length > 0) {
    process.nextTick(function () {
      self.quit(callback);
    });
  } else {
    callback();
  }
};

// This is the main runner method, where jobs execution takes place.
// Every call to this method will result into a child_process being forked.
WorkflowRunner.prototype.runJob = function (job, callback) {
  var self = this,
  wf_job_runner = new WorkflowJobRunner({
    runner: self,
    backend: self.backend,
    job: job,
    trace: self.trace,
    sandbox: self.sandbox
  });
  wf_job_runner.run(callback);
};

WorkflowRunner.prototype.getIdentifier = function (callback) {
  var cfg_file = path.resolve(__dirname, '../workflow-indentifier');

  path.exists(cfg_file, function (exist) {
    if (exist) {
      fs.readFile(cfg_file, 'utf-8', function (err, data) {
        if (err) {
          return callback(err);
        }
        return callback(null, data);
      });
    } else {
      var id = uuid();
      fs.writeFile(cfg_file, id, 'utf-8', function (err) {
        if (err) {
          return callback(err);
        }
        return callback(null, id);
      });
    }
  });
};

WorkflowRunner.prototype.runNow = function (job) {
  return (new Date().getTime() >= new Date(job.exec_after).getTime());
};

// Just in case we need to kill it without leaving child processes around:
WorkflowRunner.prototype.kill = function (callback) {
  var self = this;
  self.shutting_down = true;
  clearInterval(self.interval);
  if (Object.keys(self.child_processes).length > 0) {
    Object.keys(self.child_processes).forEach(function (p) {
      process.kill(p, 'SIGKILL');
    });
    self.child_processes = [];
  }
  if (callback) {
    callback();
  }
};


WorkflowRunner.prototype.run = function () {
  var self = this,
  // Queue worker. Tries to run a job, including "hiding" it from other
  // runners:
  worker = function (uuid, callback) {
    self.backend.getJob(uuid, function (err, job) {
      if (err) {
        callback(err);
      }

      if (self.runNow(job)) {
        self.backend.runJob(uuid, self.identifier, function (err, job) {
          if (err) {
            callback(err);
          }

          if (self.trace) {
            console.time('JOB ' + job.uuid);
          }
          self.runJob(job, function (err) {
            if (self.trace) {
              console.timeEnd('JOB ' + job.uuid);
            }
            if (err) {
              callback(err);
            }
            callback();
          });
        });
      }
    });
  },
  // We keep a queue with concurrency limit where we'll be pushing new jobs
  queue = async.queue(worker, self.forks);

  self.interval = setInterval(function () {
    self.backend.runnerActive(self.identifier, function (err) {
      if (err) {
        self.log.error(err, 'Error reporting runner activity');
        return;
      }
      self.backend.isRunnerIdle(self.identifier, function (idle) {
        if (idle === false) {
          async.parallel({
            // Fetch stale jobs from runners which stopped reporting
            // activity and cancel them:
            stale_jobs: function (cb) {
              self.staleJobs(function (err, jobs) {
                if (err) {
                  self.log.error(err, 'Error fetching stale jobs');
                  // We will not stop even on error:
                  return cb(null, null);
                }
                return async.forEach(jobs, function (uuid, fe_cb) {
                  self.backend.updateJobProperty(
                    uuid, 'execution', 'canceled',
                    function (err) {
                      if (err) {
                        return fe_cb(err);
                      }
                      return self.backend.getJob(uuid, function (err, job) {
                        if (err) {
                          return fe_cb(err);
                        }
                        return self.backend.finishJob(job, function (err, job) {
                          if (err) {
                            return fe_cb(err);
                          }
                          self.log.info('Stale Job ' + job.uuid + ' canceled');
                          return fe_cb(null);
                        });
                      });
                    });
                }, function (err) {
                  return cb(null, null);
                });
              });
            },
            // Fetch jobs to process.
            fetch_jobs: function (cb) {
              var fetch = self.forks - Object.keys(self.child_processes).length;
              self.backend.nextJobs(0, fetch - 1, function (err, jobs) {
                // Error fetching jobs
                if (err) {
                  self.log.error(err, 'Error fetching jobs');
                  // We will not stop even on error:
                  return cb(null, null);
                }
                // No queued jobs
                if (!jobs) {
                  self.log.info('No jobs queued');
                  return cb(null, null);
                }
                // Got jobs, let's see if we can run them:
                jobs.forEach(function (job) {
                  queue.push(job, function (err) {
                    // Called once queue worker finished processing the job
                    if (err) {
                      self.log.error(err, 'Error running job');
                    }
                    self.log.info('Job with uuid ' + job + ' ran successfully');
                  });
                });
                return cb(null, null);
              });

            }
          }, function (err, results) {
            return;
          });
        } else {
          self.log.info('Runner idle.');
        }
      });
    });
  }, self.run_interval);
};


WorkflowRunner.prototype.childUp = function (job_uuid, child_pid) {
  var self = this;
  self.child_processes[child_pid] = job_uuid;
};

WorkflowRunner.prototype.childDown = function (job_uuid, child_pid) {
  var self = this;
  delete self.child_processes[child_pid];
};

// Check for inactive runners.
// "Inactive runner" - a runner with latest status report older than
//  3 times the runners' run_interval.
//  - callback - f(err, runners): `err` always means backend error.
//    `runners` will be an array, even empty.
WorkflowRunner.prototype.inactiveRunners = function (callback) {
  var self = this,
      inactiveRunners = [];
  self.backend.getRunners(function (err, runners) {
    if (err) {
      return callback(err);
    }
    Object.keys(runners).forEach(function (id) {
      var outdated = new Date().getTime() - (self.run_interval * 3000);
      if (
        id !== self.identifier &&
        new Date(runners[id]).getTime() < outdated) {
        inactiveRunners.push(id);
      }
    });
    return callback(null, inactiveRunners);
  });
};


// Check for "stale" jobs, i.e, associated with inactive runners.
// - callback(err, jobs): `err` means backend error.
//   `jobs` will be an array, even empty. Note this will be an
//   array of jobs UUIDs, so they can be canceled.
WorkflowRunner.prototype.staleJobs = function (callback) {
  var self = this,
      staleJobs = [];
  self.inactiveRunners(function (err, runners) {
    if (err) {
      return callback(err);
    }
    if (runners.length === 0) {
      return callback(null, staleJobs);
    } else {
      return async.forEach(runners, function (runner, cb) {
        self.backend.getRunnerJobs(runner, function (err, jobs) {
          if (err) {
            cb(err);
          }
          staleJobs = staleJobs.concat(jobs);
          cb();
        });
      }, function (err) {
        if (err) {
          return callback(err);
        }
        return callback(null, staleJobs);
      });
    }
  });
};
