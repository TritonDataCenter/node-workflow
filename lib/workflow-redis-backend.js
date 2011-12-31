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


  self.client.on('connect', callback);

  self.client.on('error', function(err) {
    console.error('Redis error => ' + err.name + ':' + err.message);
  });
};

// Callback - f(err, res);
WorkflowRedisBackend.prototype.quit = function(callback) {
  var self = this;
  self.client.quit(callback);
};

// task - Task object
// callback - f(err, task)
WorkflowRedisBackend.prototype.createTask = function(task, callback) {
  var self = this,
      multi = self.client.multi();

  // Save the task as a Hash
  multi.hmset('task:' + task.uuid, task);
  // Add the name to the wf_task_names set in order to be able to check with
  //    SISMEMBER wf_task_names task.name
  multi.sadd('wf_task_names', task.name);

  // Validate there is not another task with the same name
  self.client.sismember('wf_task_names', task.name, function(err, result) {
    if (err) {
      return callback(err);
    }

    if (result === 1) {
      return callback('Task.name must be unique. A task with name "' +
        task.name + '" already exists');
    }

    // Really execute everything on a transaction:
    multi.exec(function(err, replies) {
      // console.log(replies)); => [ 'OK', 0 ]
      if (err) {
        return callback(err);
      } else {
        return callback(null, task);
      }
    });

  });
};

// task - update task object.
// callback - f(err, task)
WorkflowRedisBackend.prototype.updateTask = function(task, callback) {
  var self = this,
      multi = self.client.multi(),
      aTask; // We will use this variable to set the original task values
             // before the update, to enforce name uniqueness

  // Save the task as a Hash
  multi.hmset('task:' + task.uuid, task);

  self.client.exists('task:' + task.uuid, function(err, result) {
    if (err) {
      return callback(err);
    }

    if (result === 0) {
      return callback('Task does not exist. Cannot Update.');
    }

    self.getTask(task.uuid, function(err, result) {
      if (err) {
        return callback(err);
      }
      aTask = result;
      self.client.sismember('wf_task_names', task.name, function(err, result) {
        if (err) {
          return callback(err);
        }

        if (result === 1 && aTask.name !== task.name) {
          return callback('Task.name must be unique. A task with name "' +
            task.name + '" already exists');
        }

        if (aTask.name !== task.name) {
          // Remove previous name, add the new one:
          multi.srem('wf_task_names', aTask.name);
          multi.sadd('wf_task_names', task.name);
        }

        // Really execute everything on a transaction:
        multi.exec(function(err, replies) {
          // console.log(replies)); => [ 'OK', 0 ]
          if (err) {
            return callback(err);
          } else {
            return callback(null, task);
          }
        });

      });
    });
  });

};

// task - the task object
// callback - f(err, boolean)
WorkflowRedisBackend.prototype.deleteTask = function(task, callback) {
  var self = this,
      multi = self.client.multi();

  // Referential integrity: let's get all the workflows this task has been used
  // for and remove the task from their chain:
  self.client.smembers(
      'wf_task_workflows_chain:' + task.uuid,
      function(err, members)
  {
    if (err) {
      return callback(err);
    }
    async.forEachLimit(members, 10, function(wf, cb) {
      self.client.hget('workflow:' + wf, 'chain', function(err, res) {
        if (err) {
          cb(err);
        }
        if (res) {
          //var chain = JSON.parse(res);
          var chain = res.filter(function(t) {
            return (t !== task.uuid);
          });
          multi.hset('workflow:' + wf, 'chain', chain);
        }
        cb();
      });
    }, function(err) {
      if (err) {
        return callback(err);
      }

      self.client.smembers('wf_task_workflows_onerror:' +
        task.uuid,
        function(err, members) {
          if (err) {
            return callback(err);
          }
          async.forEachLimit(members, 10, function(wf, cb) {
            self.client.hget('workflow:' + wf, 'onerror', function(err, res) {
              if (err) {
                cb(err);
              }
              if (res) {
                //var onerror = JSON.parse(res);
                var onerror = res.filter(function(t) {
                  return (t !== task.uuid);
                });
                multi.hset('workflow:' + wf, 'onerror', onerror);
              }
              cb();
            });
          }, function(err) {
            if (err) {
              return callback(err);
            }
            multi.del('task:' + task.uuid);
            multi.srem('wf_task_names', task.name);
            multi.exec(function(err, replies) {
              // console.log(replies); => [ 1, 1 ]
              if (err) {
                return callback(err);
              } else {
                return callback(null, true);
              }
            });

          });
      });
    });
  });
};

// uuid - Task.uuid
// callback - f(err, task)
WorkflowRedisBackend.prototype.getTask = function(uuid, callback) {
  var self = this;

  self.client.hgetall('task:' + uuid, function(err, task) {
    if (err) {
      return callback(err);
    } else {
      return callback(null, task);
    }
  });
};


// workflow - Workflow object
// callback - f(err, workflow)
WorkflowRedisBackend.prototype.createWorkflow = function(workflow, callback) {
  var self = this,
      multi = self.client.multi();

  // Replace the workflow tasks with just a reference to every task on the
  // backend:
  if (workflow.chain.length !== 0) {
    workflow.chain.forEach(function(task, i, arr) {
      if (typeof task === 'object') {
        workflow.chain[i] = task.uuid;
        // Referential integrity for when tasks are deleted
        multi.sadd('wf_task_workflows_chain:' + task.uuid, workflow.uuid);
      } else {
        // Sets will save a single copy of its elements so is safe to proceed
        multi.sadd('wf_task_workflows_chain:' + task, workflow.uuid);
      }
    });
  }

  if (workflow.onerror && workflow.onerror.length !== 0) {
    workflow.onerror.forEach(function(task, i, arr) {
      if (typeof task === 'object') {
        workflow.onerror[i] = task.uuid;
        multi.sadd('wf_task_workflows_onerror:' + task.uuid, workflow.uuid);
      } else {
        multi.sadd('wf_task_workflows_onerror:' + task, workflow.uuid);
      }
    });
  }

  // TODO: A good place to verify that the same tasks are not on the chain
  // and into the onerror callback.

  // Save the workflow as a Hash
  multi.hmset('workflow:' + workflow.uuid, workflow);
  // Add the name to the wf_workflow_names set in order to be able to check with
  //    SISMEMBER wf_workflow_names workflow.name
  multi.sadd('wf_workflow_names', workflow.name);

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
      return callback(null, workflow);
    }
  });
};

// workflow - the workflow object
// callback - f(err, boolean)
WorkflowRedisBackend.prototype.deleteWorkflow = function(workflow, callback) {
  var self = this,
      multi = self.client.multi();

  // Referential integrity with tasks
  if (workflow.chain.length) {
    workflow.chain.forEach(function(task) {
      if (typeof task === 'object') {
        multi.srem('wf_task_workflows_chain:' + task.uuid, workflow.uuid);
      } else {
        multi.srem('wf_task_workflows_chain:' + task, workflow.uuid);
      }
    });
  }
  if (workflow.onerror.length) {
    workflow.onerror.forEach(function(task) {
      if (typeof task === 'object') {
        multi.srem('wf_task_workflows_onerror:' + task.uuid, workflow.uuid);
      } else {
        multi.srem('wf_task_workflows_onerror:' + task, workflow.uuid);
      }
    });
  }

  multi.del('workflow:' + workflow.uuid);
  multi.srem('wf_workflow_names', workflow.name);
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
      aWorkflow;

  // Replace the workflow tasks with just a reference to every task on the
  // backend:
  if (workflow.chain.length !== 0) {
    workflow.chain.forEach(function(task, i, arr) {
      if (typeof task === 'object') {
        workflow.chain[i] = task.uuid;
      }
    });
  }

  if (workflow.onerror && workflow.onerror.length !== 0) {
    workflow.onerror.forEach(function(task, i, arr) {
      if (typeof task === 'object') {
        workflow.onerror[i] = task.uuid;
      }
    });
  }

  // Save the task as a Hash
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

          // Referential integrity with tasks:
          // First, remove the tasks present before the upgrade which are
          // gone now:
          if (typeof aWorkflow.chain === 'string') {
            aWorkflow.chain = [aWorkflow.chain];
          }
          aWorkflow.chain.forEach(function(task) {
            if (workflow.chain.indexOf(task) === -1) {
              multi.srem('wf_task_workflows_chain:' + task, workflow.uuid);
            }
          });

          if (typeof aWorkflow.onerror === 'string') {
            aWorkflow.onerror = [aWorkflow.onerror];
          }
          aWorkflow.onerror.forEach(function(task) {
            if (workflow.onerror.indexOf(task) === -1) {
              multi.srem('wf_task_workflows_onerror:' + task, workflow.uuid);
            }
          });
          // Then, flag all the present tasks to be added to the sets
          // (no problem to attempt to duplicate it, Redis Set will hold just
          // one copy of each element);
          workflow.chain.forEach(function(task) {
            multi.sadd('wf_task_workflows_chain:' + task, workflow.uuid);
          });

          workflow.onerror.forEach(function(task) {
            multi.sadd('wf_task_workflows_onerror:' + task, workflow.uuid);
          });

          // Really execute everything on a transaction:
          multi.exec(function(err, replies) {
            // console.log(replies); => [ 'OK', 0 ]
            if (err) {
              return callback(err);
            } else {
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
      multi = self.client.multi();

  // Save the job as a Hash
  multi.hmset('job:' + job.uuid, job);
  // Add the uuid to the wf_queued_jobs set in order to be able to use
  // it when we're about to run queued jobs
  multi.rpush('wf_queued_jobs', job.uuid);
  // If the job has a target, save into 'wf_target:target' to make possible
  // validation of duplicated jobs with same target:
  multi.sadd('wf_target:' + job.target, job.uuid);
  // Execute everything on a transaction:
  multi.exec(function(err, replies) {
    // console.log(replies, false, 8); => [ 'OK', 1, 1 ]
    if (err) {
      return callback(err);
    } else {
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
            aJob.params === job.params
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
WorkflowBackend.prototype.nextJob = function(index, callback) {
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

