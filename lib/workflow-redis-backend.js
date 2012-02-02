// Copyright 2012 Pedro P. Candel <kusorbox@gmail.com>. All rights reserved.
//
// TODO:
// - This should be a module, so Redis is not a dependency of workflow
// - Can do some refactoring given many of the workflow/task methods are
// very similar
var util = require('util'),
    async = require('async'),
    WorkflowBackend = require('./workflow-backend');

var WorkflowRedisBackend = module.exports = function(config) {
  WorkflowBackend.call(this);
  this.config = config;
  this.client = null;
};

util.inherits(WorkflowRedisBackend, WorkflowBackend);

WorkflowRedisBackend.prototype.init = function(callback) {
  var self = this,
      port = self.config.port || 6379,
      host = self.config.host || '127.0.0.1',
      db_num = self.config.db || 1,
      redis = require('redis');


  if (self.config.debug) {
    redis.debug_mode = true;
  }

  self.client = redis.createClient(port, host, self.config);

  if (self.config.password) {
    self.client.auth(self.config.password, function() {
      console.log('Successful authentication to Redis server');
    });
  }

  self.client.on('error', function(err) {
    console.error('Redis error => ' + err.name + ':' + err.message);
  });

  self.client.on('connect', function() {
    self.client.select(db_num, function(err, res) {
      if (err) {
        throw err;
      }
      callback();
    });
  });
};

// Callback - f(err, res);
WorkflowRedisBackend.prototype.quit = function(callback) {
  var self = this;
  self.client.quit(callback);
};


// workflow - Workflow object
// callback - f(err, workflow)
WorkflowRedisBackend.prototype.createWorkflow = function(workflow, callback) {
  var self = this,
      multi = self.client.multi(),
      p;

  // TODO: A good place to verify that the same tasks are not on the chain
  // and into the onerror callback (GH-1).


  for (p in workflow) {
    if (typeof workflow[p] === 'object') {
      workflow[p] = JSON.stringify(workflow[p]);
    }
  }
  // Save the workflow as a Hash
  multi.hmset('workflow:' + workflow.uuid, workflow);
  // Add the name to the wf_workflow_names set in order to be able to check with
  //    SISMEMBER wf_workflow_names workflow.name
  multi.sadd('wf_workflow_names', workflow.name);
  multi.sadd('wf_workflows', workflow.uuid);

  // Validate there is not another workflow with the same name
  self.client.sismember(
    'wf_workflow_names',
    workflow.name,
    function(err, result) {
      if (err) {
        return callback(err);
      }

      if (result === 1) {
        return callback('Workflow.name must be unique. A workflow with name "' +
          workflow.name + '" already exists');
      }

      // Really execute everything on a transaction:
      multi.exec(function(err, replies) {
        // console.log(replies); => [ 'OK', 0 ]
        if (err) {
          return callback(err);
        } else {
          if (workflow.chain) {
            workflow.chain = JSON.parse(workflow.chain);
          }
          if (workflow.onerror) {
            workflow.onerror = JSON.parse(workflow.onerror);
          }
          return callback(null, workflow);
        }
      });

    });
};

// uuid - Workflow.uuid
// callback - f(err, workflow)
WorkflowRedisBackend.prototype.getWorkflow = function(uuid, callback) {
  var self = this;

  self.client.hgetall('workflow:' + uuid, function(err, workflow) {
    if (err) {
      return callback(err);
    } else {
      if (workflow.chain) {
        workflow.chain = JSON.parse(workflow.chain);
      }
      if (workflow.onerror) {
        workflow.onerror = JSON.parse(workflow.onerror);
      }
      return callback(null, workflow);
    }
  });
};

// workflow - the workflow object
// callback - f(err, boolean)
WorkflowRedisBackend.prototype.deleteWorkflow = function(workflow, callback) {
  var self = this,
      multi = self.client.multi();

  multi.del('workflow:' + workflow.uuid);
  multi.srem('wf_workflow_names', workflow.name);
  multi.srem('wf_workflows', workflow.uuid);
  multi.exec(function(err, replies) {
    // console.log(replies); => [ 1, 1 ]
    if (err) {
      return callback(err);
    } else {
      return callback(null, true);
    }
  });
};

// workflow - update workflow object.
// callback - f(err, workflow)
WorkflowRedisBackend.prototype.updateWorkflow = function(workflow, callback) {
  var self = this,
      multi = self.client.multi(),
      // We will use this variable to set the original workflow values
      // before the update, to enforce name uniqueness
      aWorkflow, p;

  // TODO: A good place to verify that the same tasks are not on the chain
  // and into the onerror callback (GH-1).


  for (p in workflow) {
    if (typeof workflow[p] === 'object') {
      workflow[p] = JSON.stringify(workflow[p]);
    }
  }
  // Save the workflow as a Hash
  multi.hmset('workflow:' + workflow.uuid, workflow);

  self.client.exists('workflow:' + workflow.uuid, function(err, result) {
    if (err) {
      return callback(err);
    }

    if (result === 0) {
      return callback('Workflow does not exist. Cannot Update.');
    }

    self.getWorkflow(workflow.uuid, function(err, result) {
      if (err) {
        return callback(err);
      }
      aWorkflow = result;
      self.client.sismember(
        'wf_workflow_names',
        workflow.name,
        function(err, result) {
          if (err) {
            return callback(err);
          }

          if (result === 1 && aWorkflow.name !== workflow.name) {
            return callback(
              'Workflow.name must be unique. A workflow with name "' +
              workflow.name + '" already exists'
            );
          }

          if (aWorkflow.name !== workflow.name) {
            // Remove previous name, add the new one:
            multi.srem('wf_workflow_names', aWorkflow.name);
            multi.sadd('wf_workflow_names', workflow.name);
          }

          // Really execute everything on a transaction:
          multi.exec(function(err, replies) {
            // console.log(replies); => [ 'OK', 0 ]
            if (err) {
              return callback(err);
            } else {
              if (workflow.chain) {
                workflow.chain = JSON.parse(workflow.chain);
              }
              if (workflow.onerror) {
                workflow.onerror = JSON.parse(workflow.onerror);
              }
              return callback(null, workflow);
            }
          });

        });
    });
  });

};

// job - Job object
// callback - f(err, job)
WorkflowRedisBackend.prototype.createJob = function(job, callback) {
  var self = this,
      multi = self.client.multi(),
      p;

  for (p in job) {
    if (typeof job[p] === 'object') {
      job[p] = JSON.stringify(job[p]);
    }
  }
  // Save the job as a Hash
  multi.hmset('job:' + job.uuid, job);
  // Add the uuid to the wf_queued_jobs set in order to be able to use
  // it when we're about to run queued jobs
  multi.rpush('wf_queued_jobs', job.uuid);
  multi.sadd('wf_jobs', job.uuid);
  // If the job has a target, save into 'wf_target:target' to make possible
  // validation of duplicated jobs with same target:
  multi.sadd('wf_target:' + job.target, job.uuid);
  // Execute everything on a transaction:
  multi.exec(function(err, replies) {
    // console.log(replies, false, 8); => [ 'OK', 1, 1 ]
    if (err) {
      return callback(err);
    } else {
      if (job.chain) {
        job.chain = JSON.parse(job.chain);
      }
      if (job.onerror) {
        job.onerror = JSON.parse(job.onerror);
      }
      if (job.chain_results) {
        job.chain_results = JSON.parse(job.chain_results);
      }
      if (job.onerror_results) {
        job.onerror_results = JSON.parse(job.onerror_results);
      }
      if (job.params) {
        job.params = JSON.parse(job.params);
      }
      return callback(null, job);
    }
  });
};


// uuid - Job.uuid
// callback - f(err, job)
WorkflowRedisBackend.prototype.getJob = function(uuid, callback) {
  var self = this;

  self.client.hgetall('job:' + uuid, function(err, job) {
    if (err) {
      return callback(err);
    } else {
      if (job.chain) {
        job.chain = JSON.parse(job.chain);
      }
      if (job.onerror) {
        job.onerror = JSON.parse(job.onerror);
      }
      if (job.chain_results) {
        job.chain_results = JSON.parse(job.chain_results);
      }
      if (job.onerror_results) {
        job.onerror_results = JSON.parse(job.onerror_results);
      }
      if (job.params) {
        job.params = JSON.parse(job.params);
      }
      return callback(null, job);
    }
  });
};

// job - the job object
// callback - f(err) called with error in case there is a duplicated
// job with the same target and same params
WorkflowRedisBackend.prototype.validateJobTarget = function(job, callback) {
  var self = this;
  // If no target is given, we don't care:
  if (!job.target) {
    return callback(null);
  }

  self.client.smembers('wf_target:' + job.target, function(err, members) {
    if (members.length === 0) {
      return callback(null);
    }
    // We have an array of jobs uuids with the same target. Need to verify
    // none of them has the same parameters than the job we're trying to
    // queue:
    // (NOTE: Make the limit of concurrent connections to Redis configurable)
    async.forEachLimit(members, 10, function(uuid, cb) {
      self.getJob(uuid, function(err, aJob) {
        if (err) {
          cb(err);
        } else {
          if (
            aJob.workflow_uuid === job.workflow_uuid &&
            JSON.stringify(aJob.params) === JSON.stringify(job.params)
          ) {
            // Already got same target, now also same workflow and same params
            // fail it
            cb(
              'Another job with the same target and params is already queued'
            );
          } else {
            cb();
          }
        }
      });
    }, function(err) {
      if (err) {
        return callback(err);
      }
      return callback(null);
    });
  });
};

// Get the next queued job.
// index - Integer, optional. When given, it'll get the job at index position
//         (when not given, it'll return the job at position zero).
// callback - f(err, job)
WorkflowRedisBackend.prototype.nextJob = function(index, callback) {
  var self = this;

  if (typeof index === 'function') {
    callback = index;
    index = 0;
  }

  self.client.lrange('wf_queued_jobs', index, index, function(err, res) {
    if (err) {
      return callback(err);
    }

    if (res.length === 0) {
      return callback(null, null);
    }

    return self.getJob(res[0], callback);
  });
};


// Lock a job, mark it as running by the given runner, update job status.
// uuid - the job uuid (String)
// runner_id - the runner identifier (String)
// callback - f(err) callback will be called with error if something fails,
//            otherwise it'll called with null.
WorkflowRedisBackend.prototype.runJob = function(uuid, runner_id, callback) {
  var self = this,
      multi = self.client.multi();

  self.client.lrem('wf_queued_jobs', 0, uuid, function(err, res) {
    if (err) {
      return callback(err);
    }

    if (res <= 0) {
      return callback('Only queued jobs can be run');
    }

    multi.sadd('wf_runner:' + runner_id, uuid);
    multi.rpush('wf_running_jobs', uuid);
    multi.hset('job:' + uuid, 'execution', 'running');
    multi.hset('job:' + uuid, 'runner', runner_id);
    multi.exec(function(err, replies) {
      if (err) {
        return callback(err);
      } else {
        return callback(null);
      }
    });
  });
};

// Unlock the job, mark it as finished, update the status, add the results
// for every job's task.
// job - the job object. It'll be saved to the backend with the provided
//       properties.
// callback - f(err) called with error if something fails, otherwise with null.
WorkflowRedisBackend.prototype.finishJob = function(job, callback) {
  var self = this,
      multi = self.client.multi(),
      p;

  for (p in job) {
    if (typeof job[p] === 'object') {
      job[p] = JSON.stringify(job[p]);
    }
  }

  self.client.lrem('wf_running_jobs', 0, job.uuid, function(err, res) {
    if (err) {
      return callback(err);
    }

    if (res <= 0) {
      return callback('Only running jobs can be finished');
    }
    if (job.execution === 'running') {
      job.execution = 'succeeded';
    }
    multi.srem('wf_runner:' + job.runner, job.uuid);
    if (job.execution === 'succeeded') {
      multi.rpush('wf_succeeded_jobs', job.uuid);
    } else {
      multi.rpush('wf_failed_jobs', job.uuid);
    }

    multi.hmset('job:' + job.uuid, job);
    multi.hdel('job:' + job.uuid, 'runner');
    multi.exec(function(err, replies) {
      if (err) {
        return callback(err);
      } else {
        if (job.chain) {
          job.chain = JSON.parse(job.chain);
        }
        if (job.onerror) {
          job.onerror = JSON.parse(job.onerror);
        }
        if (job.chain_results) {
          job.chain_results = JSON.parse(job.chain_results);
        }
        if (job.onerror_results) {
          job.onerror_results = JSON.parse(job.onerror_results);
        }
        if (job.params) {
          job.params = JSON.parse(job.params);
        }
        return callback(null);
      }
    });
  });
};

// Update the job while it is running with information regarding progress
// job - the job object. It'll be saved to the backend with the provided
//       properties.
// callback - f(err) called with error if something fails, otherwise with null.
WorkflowRedisBackend.prototype.updateJob = function(job, callback) {
  var self = this,
      p;

  for (p in job) {
    if (typeof job[p] === 'object') {
      job[p] = JSON.stringify(job[p]);
    }
  }
  self.client.hmset('job:' + job.uuid, job, function(err, res) {
    if (err) {
      return callback(err);
    }
    return callback();
  });
};

// Queue a job which has been running; i.e, due to whatever the reason,
// re-queue the job. It'll unlock the job, update the status, add the
// results for every finished task so far ...
// job - the job Object. It'll be saved to the backend with the provided
//       properties to ensure job status persistence.
// callback - f(err) same approach, if something fails called with error.
WorkflowRedisBackend.prototype.queueJob = function(job, callback) {
  var self = this,
      multi = self.client.multi(),
      p;

  for (p in job) {
    if (typeof job[p] === 'object') {
      job[p] = JSON.stringify(job[p]);
    }
  }

  self.client.lrem('wf_running_jobs', 0, job.uuid, function(err, res) {
    if (err) {
      return callback(err);
    }

    if (res <= 0) {
      return callback('Only running jobs can be queued again');
    }
    job.execution = 'queued';
    multi.srem('wf_runner:' + job.runner, job.uuid);
    multi.rpush('wf_queued_jobs', job.uuid);
    multi.hmset('job:' + job.uuid, job);
    multi.hdel('job:' + job.uuid, 'runner');
    multi.exec(function(err, replies) {
      if (err) {
        return callback(err);
      } else {
        if (job.chain) {
          job.chain = JSON.parse(job.chain);
        }
        if (job.onerror) {
          job.onerror = JSON.parse(job.onerror);
        }
        if (job.chain_results) {
          job.chain_results = JSON.parse(job.chain_results);
        }
        if (job.onerror_results) {
          job.onerror_results = JSON.parse(job.onerror_results);
        }
        if (job.params) {
          job.params = JSON.parse(job.params);
        }
        return callback(null);
      }
    });
  });
};


// Get the given number of queued jobs uuids.
// - start - Integer - Position of the first job to retrieve
// - stop - Integer - Position of the last job to retrieve, _included_
// - callback - f(err, jobs)
// See http://redis.io/commands/lrange for the details about start/stop.
WorkflowRedisBackend.prototype.nextJobs = function(start, stop, callback) {
  var self = this;

  self.client.lrange('wf_queued_jobs', start, stop, function(err, res) {
    if (err) {
      return callback(err);
    }

    if (res.length === 0) {
      return callback(null, null);
    }

    return callback(null, res);
  });
};

// Register a runner on the backend and report it's active:
// - runner_id - String, unique identifier for runner.
// - callback - f(err)
WorkflowRedisBackend.prototype.registerRunner = function(runner_id, callback) {
  var self = this;
  self.client.hset(
    'wf_runners',
    runner_id,
    new Date().toISOString(),
    function(err, res) {
      if (err) {
        return callback(err);
      }
      // Actually, we don't care at all about the 0/1 possible return values.
      return callback(null);
    });
};

// Report a runner remains active:
// - runner_id - String, unique identifier for runner.
// - callback - f(err)
WorkflowRedisBackend.prototype.runnerActive = function(runner_id, callback) {
  var self = this;
  return self.registerRunner(runner_id, callback);
};

// Get all the registered runners:
// - callback - f(err, runners)
WorkflowRedisBackend.prototype.getRunners = function(callback) {
  var self = this;
  return self.client.hgetall('wf_runners', callback);
};

// Get all the workflows:
// - callback - f(err, workflows)
WorkflowRedisBackend.prototype.getWorkflows = function(callback) {
  var self = this,
      multi = self.client.multi();

  self.client.smembers('wf_workflows', function(err, res) {
    if (err) {
      return callback(err);
    }
    res.forEach(function(uuid) {
      multi.hgetall('workflow:' + uuid);
    });
    multi.exec(function(err, replies) {
      if (err) {
        return callback(err);
      }
      replies.forEach(function(workflow, i, arr) {
        if (workflow.chain) {
          workflow.chain = JSON.parse(workflow.chain);
        }
        if (workflow.onerror) {
          workflow.onerror = JSON.parse(workflow.onerror);
        }
        replies[i] = workflow;
      });
      return callback(null, replies);
    });
  });
};


// Get all the jobs:
// - execution - String, the execution status for the jobs to return.
//               Return all jobs if no execution status is given.
// - callback - f(err, jobs)
WorkflowRedisBackend.prototype.getJobs = function(execution, callback) {
  var self = this,
      multi = self.client.multi(),
      executions = ['queued', 'failed', 'succeeded', 'running'],
      list_name;

  if (typeof execution === 'function') {
    callback = execution;
    self.client.smembers('wf_jobs', function(err, res) {
      res.forEach(function(uuid) {
        multi.hgetall('job:' + uuid);
      });

      multi.exec(function(err, replies) {
        if (err) {
          return callback(err);
        }
        replies.forEach(function(job, i, arr) {
          if (job.chain) {
            job.chain = JSON.parse(job.chain);
          }
          if (job.onerror) {
            job.onerror = JSON.parse(job.onerror);
          }
          if (job.chain_results) {
            job.chain_results = JSON.parse(job.chain_results);
          }
          if (job.onerror_results) {
            job.onerror_results = JSON.parse(job.onerror_results);
          }
          if (job.params) {
            job.params = JSON.parse(job.params);
          }
          replies[i] = job;
        });
        return callback(null, replies);
      });
    });
  } else if (executions.indexOf(execution !== -1)) {
    list_name = 'wf_' + execution + '_jobs';
    self.client.llen(list_name, function(err, res) {
      if (err) {
        return callback(err);
      }
      self.client.lrange(list_name, 0, (res + 1), function(err, results) {
        if (err) {
          return callback(err);
        }
        results.forEach(function(uuid) {
          multi.hgetall('job:' + uuid);
        });
        multi.exec(function(err, replies) {
          if (err) {
            return callback(err);
          }
          replies.forEach(function(job, i, arr) {
            if (job.chain) {
              job.chain = JSON.parse(job.chain);
            }
            if (job.onerror) {
              job.onerror = JSON.parse(job.onerror);
            }
            if (job.chain_results) {
              job.chain_results = JSON.parse(job.chain_results);
            }
            if (job.onerror_results) {
              job.onerror_results = JSON.parse(job.onerror_results);
            }
            if (job.params) {
              job.params = JSON.parse(job.params);
            }
            replies[i] = job;
          });
          return callback(null, replies);
        });
      });
    });
  } else {
    return callback('excution is required and must be one of' +
        '"queued", "failed", "succeeded", "running"');
  }
};


// Add progress information to an existing job:
// - uuid - String, the Job's UUID.
// - info - Object, {'key' => 'Value'}
// - callback - f(err)
WorkflowRedisBackend.prototype.addInfo = function(uuid, info, callback) {
  var self = this;
  if (typeof info === 'object') {
    info = JSON.stringify(info);
  }

  self.client.exists('job:' + uuid, function(err, result) {
    if (err) {
      return callback(err);
    }

    if (result === 0) {
      return callback('Job does not exist. Cannot Update.');
    }

    self.client.rpush('jobinfo:' + uuid, info, function(err, res) {
      if (err) {
        return callback(err);
      }
      return callback();
    });
  });
};


// Get progress information from an existing job:
// - uuid - String, the Job's UUID.
// - callback - f(err, info)
WorkflowRedisBackend.prototype.getInfo = function(uuid, callback) {
  var self = this,
      llen,
      info = [];

  self.client.exists('job:' + uuid, function(err, result) {
    if (err) {
      return callback(err);
    }

    if (result === 0) {
      return callback('Job does not exist. Cannot get info.');
    }

    self.client.llen('jobinfo:' + uuid, function(err, res) {
      if (err) {
        return callback(err);
      }
      llen = res;
      self.client.lrange('jobinfo:' + uuid, 0, llen, function(err, items) {
        if (err) {
          return callback(err);
        }
        if (items.length) {
          items.forEach(function(item) {
            info.push(JSON.parse(item));
          });
        }
        return callback(null, info);
      });
    });

  });

};
