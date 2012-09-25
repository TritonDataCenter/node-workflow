// Copyright 2012 Pedro P. Candel <kusorbox@gmail.com> All rights reserved.
var util = require('util'),
    uuid = require('node-uuid'),
    fs = require('fs'),
    path = require('path'),
    async = require('async'),
    Logger = require('bunyan'),
    WorkflowJobRunner = require('./job-runner'),
    exists = fs.exists || path.exists;

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

    if (typeof (opts.dtrace) !== 'object') {
        throw new TypeError('opts.dtrace (Object) required');
    }

    if (typeof (opts.runner) !== 'object') {
        opts.runner = {};
    }

    // Make sure both are numeric:
    if (typeof (opts.runner.forks) !== 'number' || opts.runner.forks <= 1) {
        opts.runner.forks = 5;
    } else {
        opts.runner.forks = Math.round(opts.runner.forks);
    }

    if (typeof (opts.runner.run_interval) !== 'number' ||
        opts.runner.run_interval <= 0) {
        opts.runner.run_interval = 10;
    }

    var Backend = require(opts.backend.module);
    this.identifier = opts.runner.identifier || null;
    this.forks = opts.runner.forks;
    this.run_interval = opts.runner.run_interval * 1000;
    this.interval = null;
    this.shutting_down = false;
    this.child_processes = {};
    this.sandbox = opts.runner.sandbox || {};
    this.slots = opts.runner.forks;
    this.rn_jobs = [];
    this.job_runners = {};
    this.dtrace = opts.dtrace;

    if (opts.log) {
        this.log = opts.log({
            component: 'workflow-runner',
            runner_uuid: this.identifier
        });
    } else {
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
    }
    opts.backend.log = this.log;
    this.backend = new Backend(opts.backend.opts);
};

WorkflowRunner.prototype.init = function (callback) {
    var self = this,
        series = [];
    // Job uuid, job name, job execution, job target,
    // job params, timeout & time elapsed
    self.dtrace.addProbe('wf-job-start',
                         'char *',
                         'char *',
                         'char *',
                         'char *',
                         'char *',
                         'int',
                         'char *');
    // Job uuid, job name, job execution, job target,
    // job params, timeout & time elapsed
    self.dtrace.addProbe('wf-job-done',
                         'char *',
                         'char *',
                         'char *',
                         'char *',
                         'char *',
                         'int',
                         'char *');
    // Task name, task body, start time (time we fire start probe)
    self.dtrace.addProbe('wf-task-start',
                        'char *',
                        'char *',
                        'int');
    // Task name, result, error, started_at/finished_at,
    // end time (time we fire done probe):
    self.dtrace.addProbe('wf-task-done',
                        'char *',
                        'char *',
                        'char *',
                        'int',
                        'int',
                        'int');
    // Enable dtrace provider first:
    try {
        self.dtrace.enable();
    } catch (e) {
        self.log.error({err: e}, 'Failed to enable DTrace Provider');
    }
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
    series.push(function (cb) {
        self.backend.getRunnerJobs(self.identifier, function (err, jobs) {
            if (err) {
                return cb(err);
            } else {
                return async.forEach(jobs, function (uuid, next_cb) {
                    self.backend.updateJobProperty(uuid,
                      'execution',
                      'canceled',
                      function (err) {
                        if (err) {
                            return next_cb(err);
                        }
                        return next_cb(null);
                      });
                }, function (err) {
                    if (err) {
                        return cb(err);
                    }
                    return cb(null, null);
                });
            }
        });
    });
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
    if (self.rn_jobs.length > 0) {
        self.rn_jobs.forEach(function (j) {
            self.job_runners[j].cancel('queued', function (err) {
                if (err) {
                    self.log.error({err: err}, 'Error trying to cancel job');
                } else {
                    self.log.info('Job with UUID ' + j + ' canceled.');
                }
            });
        });
    }
    if (self.slots < self.forks) {
        setTimeout(function () {
            self.quit(callback);
        }, self.run_interval);
    } else {
        callback();
    }
};

// This is the main runner method, where jobs execution takes place.
// Every call to this method will result into a child_process being forked.
WorkflowRunner.prototype.runJob = function (job, callback) {
    var self = this;
    self.job_runners[job.uuid] = new WorkflowJobRunner({
        runner: self,
        backend: self.backend,
        job: job,
        trace: self.log.trace(),
        sandbox: self.sandbox,
        dtrace: self.dtrace
    });
    self.job_runners[job.uuid].run(callback);
};

WorkflowRunner.prototype.getIdentifier = function (callback) {
    var cfg_file = path.resolve(__dirname, '../workflow-indentifier');

    exists(cfg_file, function (exist) {
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
    if (self.childCount() > 0) {
        Object.keys(self.child_processes).forEach(function (p) {
            process.kill(p, 'SIGKILL');
        });
        self.child_processes = {};
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
        return self.backend.getJob(uuid, function (err, job) {
            if (err) {
                return callback(err);
            }

            if (!job) {
                return callback();
            }

            if (self.runNow(job)) {
                return self.backend.runJob(uuid, self.identifier,
                  function (err, job) {
                    if (err) {
                        return callback(err);
                    }

                    if (!job) {
                        return callback();
                    }

                    var t = Date.now();
                    self.dtrace.fire('wf-job-start', function jProbeStart() {
                        var ret = [
                            job.uuid,
                            job.name,
                            job.execution,
                            job.target,
                            JSON.stringify(job.params),
                            job.timeout,
                            String()
                        ];
                        return (ret);
                    });
                    return self.runJob(job, function (err) {
                        self.dtrace.fire('wf-job-done', function jProbeDone() {
                            var ret = [
                                job.uuid,
                                job.name,
                                job.execution,
                                job.target,
                                JSON.stringify(job.params),
                                job.timeout,
                                String()
                            ];
                            return (ret);
                        });
                        if (self.log.trace()) {
                            self.log.trace(
                              '%s: %dms', ('JOB ' + job.uuid),
                              (Date.now() - t));
                        }

                        delete self.job_runners[job.uuid];

                        if (err) {
                            return callback(err);
                        }
                        return callback();
                    });
                });
            } else {
                return callback();
            }
        });
    },
    // We keep a queue with concurrency limit where we'll be pushing new jobs
    queue = async.queue(worker, self.forks - 1);

    self.interval = setInterval(function () {
        self.backend.runnerActive(self.identifier, function (err) {
            if (err) {
                self.log.error({err: err}, 'Error reporting runner activity');
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
                                    self.log.error({err: err},
                                      'Error fetching stale jobs');
                                    // We will not stop even on error:
                                    return cb(null, null);
                                }
                                return async.forEach(jobs,
                                  function (uuid, fe_cb) {
                                    self.backend.updateJobProperty(
                                      uuid, 'execution', 'canceled',
                                        function (err) {
                                            if (err) {
                                                return fe_cb(err);
                                            }
                                            return self.backend.getJob(uuid,
                                              function (err, job) {
                                                if (err) {
                                                    return fe_cb(err);
                                                }
                                                return self.backend.finishJob(
                                                  job, function (err, job) {
                                                    if (err) {
                                                        return fe_cb(err);
                                                    }
                                                    self.log.info(
                                                      'Stale Job ' +
                                                      job.uuid + ' canceled');
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
                            var fetch = self.slots - 1;
                            if (isNaN(fetch) || fetch <= 0) {
                                self.log.info('No available slots. ' +
                                    'Waiting next iteration');
                                return cb(null, null);
                            }
                            return self.backend.nextJobs(0, fetch,
                                function (err, jobs) {
                                // Error fetching jobs
                                if (err) {
                                    self.log.error({err: err},
                                      'Error fetching jobs');
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
                                    if (self.rn_jobs.indexOf(job) === -1) {
                                        self.rn_jobs.push(job);
                                        queue.push(job, function (err) {
                                            // Called once queue worker
                                            // finished processing the job
                                            if (err) {
                                                self.log.error({err: err},
                                                  'Error running job');
                                            }
                                            self.log.info('Job with uuid ' +
                                              job +
                                              ' ran successfully');
                                            if (self.rn_jobs.indexOf(job) !==
                                                -1) {
                                                self.rn_jobs.splice(
                                                    self.rn_jobs.indexOf(job),
                                                    1);
                                            }
                                        });
                                    }
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

WorkflowRunner.prototype.childCount = function () {
    var self = this;
    return Object.keys(self.child_processes).length;
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

WorkflowRunner.prototype.getSlot = function () {
    var self = this;
    if (self.slots === 0) {
        return false;
    } else {
        self.slots -= 1;
        return true;
    }
};

WorkflowRunner.prototype.releaseSlot = function () {
    var self = this;
    if (self.slots >= self.forks) {
        self.log.error('Attempt to release more slots than available forks');
        return false;
    } else {
        self.slots += 1;
        return true;
    }
};
