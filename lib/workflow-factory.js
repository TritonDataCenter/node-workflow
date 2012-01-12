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
// - opts - the workflow object properties:
//   - name: string workflow name, uniqueness enforced.
//   - timeout: integer, acceptable time, in minutes, to run the wf.
//     (60 minutes if nothing given).
//   - chain: An array of Tasks to run.
//   - onerror: An array of Tasks to run in case `chain` fails.
// - callback - function(err, workflow)
//
WorkflowFactory.prototype.workflow = function(opts, callback) {
  var self = this,
      wf = opts || {},
      p;

  if (!wf.name) {
    return callback('Workflow "name" is required');
  }

  if (wf.chain && (
        typeof wf.chain !== 'object' ||
        typeof wf.chain.length === 'undefined'
  )) {
    return callback('Workflow "chain" must be an array');
  }

  if (!wf.chain) {
    wf.chain = [];
  }

  if (!wf.uuid) {
    wf.uuid = uuid();
  }

  if (wf.onError) {
    wf.onerror = wf.onError;
    delete wf.onError;
  }

  if (wf.onerror && (
        typeof wf.onerror !== 'object' ||
        typeof wf.onerror.length === 'undefined'
  )) {
    return callback('Workflow "onerror" must be an array');
  }

  if (!wf.timeout) {
    wf.timeout = 60;
  }

  // NOTE: It's possible to initially have a workflow without any task on the
  // chain, and add task later using addWorkflowTask. Go for it, but remember
  // that we need to validate Jobs created from the workflows to make sure they
  // have, at least, one task on the chain; otherwise, those would be
  // nonsensical jobs.

  // opts.chain is an Array of tasks, we must replace with
  // references to the tasks keys before we store it, and those references may
  // change depending on the backend of choice.

  self.backend.createWorkflow(wf, function(err, result) {
    if (err) {
      return callback(err);
    } else {
      self._workflowCache[wf.uuid] = wf;
      return callback(null, wf);
    }
  });
};


// Create a task and store it on the backend
//
// - opts - the task object properties:
//   - name - string task name, uniqueness enforced
//   - body - function(job, cb) the task main function
//   - onerror: function(err, job, cb) a function to run in case `body` fails
//   - retry: Integer, number of attempts to run the task before try `onerror`.
//   - timeout: Integer, acceptable time, in seconds, a task execution should
//     take, before fail it with timeout error.
// - callback - function(err, task)
//
WorkflowFactory.prototype.task = function(opts, callback) {
  var self = this,
      task = opts || {},
      p;

  if (!task.name) {
    return callback('Task "name" is required');
  }

  if (!task.body) {
    return callback('Task "body" is required');
  }

  if (typeof task.body !== 'function') {
    return callback('Task "body" must be a function');
  }

  if (!task.uuid) {
    task.uuid = uuid();
  }

  if (task.onError) {
    task.onerror = task.onError;
    delete task.onError;
  }

  // Ensure that if task.onerror is given, it's a function
  if (task.onerror && typeof task.onerror !== 'function') {
    return callback('Task "onerror" must be a function');
  }

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
// - opts - the Job object workflow and extra arguments:
//   - workflow - (required) Workflow object to create the job from.
//   - params - (opt) JSON object, parameters to pass to the job during exec
//   - target - (opt) String, Job's target, used to ensure that we don't
//              queue two jobs with the same target and params at once.
//   - exec_after - (opt) ISO 8601 Date, delay job execution after the
//                  given timestamp (execute from now when not given).
// - callback - f(err, job)
WorkflowFactory.prototype.job = function(wf, opts, callback) {
  var self = this,
      job = { status: 'queued', chain_results: []},
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

