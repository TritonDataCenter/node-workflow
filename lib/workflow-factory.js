// Copyright 2012 Pedro P. Candel <kusorbox@gmail.com>. All rights reserved.
var uuid = require('node-uuid'),
    util = require('util');

var WorkflowFactory = module.exports = function(backend) {
  this.backend = backend;
  this._taskCache = {};
  this._workflowCache = {};
  this._jobCache = {};
};

// Create a workflow and store it on the backend
//
// name - string workflow name, uniqueness enforced
// opts - the workflow object properties
// callback - function(err, workflow)
//
WorkflowFactory.prototype.workflow = function(name, opts, callback) {
  var self = this,
      workflow = opts || {},
      p;

  if (!name) {
    return callback('Workflow "name" is required');
  }

  if (workflow.chain && (
        typeof workflow.chain !== 'object' ||
        typeof workflow.chain.length === 'undefined'
  )) {
    return callback('Workflow "chain" must be an array');
  }

  if (!workflow.chain) {
    workflow.chain = [];
  }

  if (!workflow.uuid) {
    workflow.uuid = uuid();
  }

  workflow.name = name;

  if (workflow.onError) {
    workflow.onerror = workflow.onError;
    delete workflow.onError;
  }

  if (workflow.onerror && (
        typeof workflow.onerror !== 'object' ||
        typeof workflow.onerror.length === 'undefined'
  )) {
    return callback('Workflow "onerror" must be an array');
  }
  // TODO: Set workflow.timeout to some safe default when not defined

  // NOTE: It's possible to initially have a workflow without any task on the
  // chain, and add task later using addWorkflowTask. Go for it, but remember
  // that we need to validate Jobs created from the workflows to make sure they
  // have, at least, one task on the chain; otherwise, those would be
  // nonsensical jobs.

  // opts.chain is an Array of tasks, we must replace with
  // references to the tasks keys before we store it, and those references may
  // change depending on the backend of choice.

  self.backend.createWorkflow(workflow, function(err, result) {
    if (err) {
      return callback(err);
    } else {
      self._workflowCache[workflow.uuid] = workflow;
      return callback(null, workflow);
    }
  });
};


// Create a task and store it on the backend
//
// name - string task name, uniqueness enforced
// opts - the task object properties
// body - function(job) the task main function
// callback - function(err, workflow)
//
WorkflowFactory.prototype.task = function(name, opts, body, callback) {
  var self = this,
      task = opts || {},
      p;

  if (!name) {
    return callback('Task "name" is required');
  }

  if (!body) {
    return callback('Task "body" is required');
  }

  if (typeof body !== 'function') {
    return callback('Task "body" must be a function');
  }

  if (!task.uuid) {
    task.uuid = uuid();
  }

  task.name = name;
  task.body = body.toString();

  for (p in task) {
    if (typeof task[p] === 'function') {
      task[p] = task[p].toString();
    }
  }

  self.backend.createTask(task, function(err, result) {
    if (err) {
      return callback(err);
    } else {
      self._taskCache[task.uuid] = task;
      return callback(null, task);
    }
  });
};


// Add a task to the given workflow chain
//
// - wf - Workflow object
// - task - task object
// - callback - f(err, workflow)
WorkflowFactory.prototype.addWorkflowTask = function(wf, task, callback) {
  var self = this;
  if (!wf.uuid || Object.keys(self._workflowCache).indexOf(wf.uuid) === -1) {
    return callback('"wf" must be an existing Workflow object');
  }

  if (!task.uuid || Object.keys(self._taskCache).indexOf(task.uuid) === -1) {
    return callback('"task" must be an existing Task object');
  }

  wf.chain.push(task);

  return self.backend.updateWorkflow(wf, callback);
};


// Remove a task from the given workflow chain
//
// - wf - Workflow object
// - task - task object
// - callback - f(err, workflow)
WorkflowFactory.prototype.removeWorkflowTask = function(wf, task, callback) {
  var self = this;
  if (!wf.uuid || Object.keys(self._workflowCache).indexOf(wf.uuid) === -1) {
    return callback('"wf" must be an existing Workflow object');
  }

  if (!task.uuid || Object.keys(self._taskCache).indexOf(task.uuid) === -1) {
    return callback('"task" must be an existing Task object');
  }

  // We may have tasks on the workflow chain as objects or referenced by uuid
  wf.chain = wf.chain.filter(function(t) {
    var uuid = (typeof t === 'object') ? t.uuid : t;
    return (task.uuid !== uuid);
  });

  return self.backend.updateWorkflow(wf, callback);
};

// Create a queue a Job from the given Workflow:
//
// - wf - Workflow object
// - opts - (opt) JSON object with members 'params', 'target', 'exec_after':
//   - params - (opt) JSON object, parameters to pass to the job during exec
//   - target - (opt) String, Job's target, used to ensure that we don't
//              queue two jobs with the same target and params at once.
//   - exec_after - (opt) ISO 8601 Date, delay job execution after the
//                  given timestamp (execute from now when not given).
// - callback - f(err, job)
WorkflowFactory.prototype.job = function(wf, opts, callback) {
  var self = this,
      job = { status: 'queued', results: []},
      p;

  if (arguments.length === 2) {
    callback = opts;
    opts = {};
  }

  if (!wf.uuid || Object.keys(self._workflowCache).indexOf(wf.uuid) === -1) {
    return callback('"wf" must be an existing Workflow object');
  }

  if (wf.chain.length === 0) {
    return callback('Cannot queue a job from a workflow without any task');
  }

  for (p in wf) {
    if (p !== 'uuid') {
      job[p] = wf[p];
    } else {
      job.workflow_uuid = wf.uuid;
    }
  }

  // job.chain & job.onerror must me complete task objects, not their
  // reference uuids.
  job.chain.forEach(function(task, i, arr) {
    if (typeof task === 'string') {
      // Yes, modifying the array as we go. Anybody knows what the ECMA spec
      // says about this?.
      arr[i] = self._taskCache[task];
    }
  });
  job.chain = JSON.stringify(job.chain);

  if (job.onerror) {
    job.onerror.forEach(function(task, i, arr) {
      if (typeof task === 'string') {
        arr[i] = self._taskCache[task];
      }
    });
    job.onerror = JSON.stringify(job.onerror);
  }

  // Blow up the error if nothing else is specified?
  // if (!job.onerror) {
  //   job.onerror = function(err) {
  //     var self = this;
  //     self.emit('error', err);
  //   }
  // }

  job.exec_after = opts.exec_after || new Date().toISOString();
  job.params = opts.params || {};

  if (typeof job.params === 'object') {
    job.params = JSON.stringify(job.params);
  }

  if (!job.uuid) {
    job.uuid = uuid();
  }

  if (opts.target) {
    job.target = opts.target;
  }

  // TODO: If target is given, search for jobs with the same wf name, the
  // same target, and verify the parameters are not the same or return error
  // (backend should take care of do not return an error if job hasn't got
  // a target property)
  self.backend.validateJobTarget(job, function(err) {
    if (err) {
      return callback(err);
    } else {
      self.backend.createJob(job, function(err, results) {
        if (err) {
          return callback(err);
        } else {
          self._jobCache[job.uuid] = job;
          return callback(null, job);
        }
      });
    }
  });
};

