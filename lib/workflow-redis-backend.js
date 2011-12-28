// Copyright 2012 Pedro P. Candel <kusorbox@gmail.com>. All rights reserved.
//
// TODO:
// - This should be a module, so Redis is not a dependency of workflow
// - Can do some refactoring given many of the workflow/task methods are
// very similar
var util = require('util'),
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
        // console.log(replies)); => [ 'OK', 0 ]
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

          // Really execute everything on a transaction:
          multi.exec(function(err, replies) {
            // console.log(replies)); => [ 'OK', 0 ]
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
