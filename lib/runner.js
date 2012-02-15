// Copyright 2012 Pedro P. Candel <kusorbox@gmail.com> All rights reserved.
var util = require('util'),
    uuid = require('node-uuid'),
    fs = require('fs'),
    path = require('path'),
    async = require('async'),
    WorkflowJobRunner = require('./job-runner');

// - opts - configuration options:
//    - identifier: Unique identifier for this runner.
//    - forks: Max number of child processes to fork at the same time.
//    - run_interval: Check for new jobs every 'run_interval' minutes.
//                    (By default, every 2 minutes).
//    - sandbox: Collection of node modules to pass to the sandboxed tasks
//               execution. Object with the form:
//               {
//                  'module_global_var_name': 'node-module-name'
//               }
//               By default, only the global timeouts are passed to the tasks
//               sandbox.
var WorkflowRunner = module.exports = function (backend, opts) {
  this.backend = backend;
  if (!opts) {
    opts = {};
  }
  this.identifier = opts.identifier || null;
  this.forks = opts.forks || 10;
  this.run_interval = (opts.run_interval || 2) * 60 * 1000;
  this.interval = null;
  this.shutting_down = false;
  this.child_processes = [];
  this.sandbox = opts.sandbox || {};
  this.trace = opts.trace || false;
  this.log = opts.log || console;
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
  async.series(series, function (err, results) {
    // Note we don't care at all about the results.
    if (err) {
      return callback(err);
    }
    return callback();
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
    trace: self.trace
  });
  wf_job_runner.run(callback);
};

WorkflowRunner.prototype.getIdentifier = function (callback) {
  var cfg_file = path.resolve(__dirname, '../config/workflow-indentifier');

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
        self.backend.runJob(uuid, self.identifier, function (err) {
          if (err) {
            callback(err);
          }
          // Given backend.runJob returns nothing, we need to update the
          // execution status here:
          if (self.trace) {
            console.time('JOB ' + job.uuid);
          }
          job.execution = 'running';
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
        self.log.error('Error reporting runner activity: ' + err);
        return;
      }
      self.backend.isRunnerIdle(self.identifier, function (idle) {
        if (idle === false) {
          var fetch = self.forks - Object.keys(self.child_processes).length;
          self.backend.nextJobs(0, fetch - 1, function (err, jobs) {
            // Error fetching jobs
            if (err) {
              self.log.error('Error fetching jobs: ' + err);
              return;
            }
            // No queued jobs
            if (!jobs) {
              self.log.info('No jobs queued');
              return;
            }
            // Got jobs, let's see if we can run them:
            jobs.forEach(function (job) {
              queue.push(job, function (err) {
                // To be called once queue worker finished processing the job
                if (err) {
                  self.log.error('Error running job: ' + err);
                }
                self.log.info('Job with uuid ' + job + ' ran successfully');
              });
            });
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


// TODO:
// - Runners should be able to pick any job on a "weird" status when
//   we detect a runner isn't upgrading its status after Y seconds
//   (also configurable), obviously Y needs to be greater than X.

if (require.main === module) {
  var Logger = require('bunyan');
  var log = new Logger({
    name: 'workflow-runner',
    streams: [ {
      level: 'info',
      stream: process.stdout
    }, {
      level: 'trace',
      path: path.resolve(__dirname, '../logs/runner.log')
    }],
    serializers: {
      err: Logger.stdSerializers.err
    }
  });
  var config_file = path.normalize(__dirname + '/../config/config.json');
  fs.readFile(config_file, 'utf8', function (err, data) {
    if (err) {
      throw err;
    }
    var config, backend, Backend, runner;
    config = JSON.parse(data);
    config.logger = log;
    Backend = require(config.backend.module);
    backend = new Backend(config.backend.opts);
    backend.init(function () {
      runner = new WorkflowRunner(backend, config.runner);
      runner.run();
    });
  });
}
