// Copyright 2012 Pedro P. Candel <kusorbox@gmail.com>. All rights reserved.
var uuid = require('node-uuid');

var WorkflowFactory = module.exports = function(backend) {
  this.backend = backend;
  this._taskCache = {};
  this._workflowCache = {};
  this._jobCache = {};
};

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
  // chain, and add task later using addTask. Go for it, but remember that we
  // need to validate Jobs created from the workflows to make sure they have,
  // at least, one task on the chain; otherwise, those would be nonsensical
  // jobs.

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

WorkflowFactory.prototype.addWorkflowTask = function(workflow, task, callback) {
  var self = this;
  workflow.addTask(task);
  // Update workflow on storage
  return callback(null, workflow);
};

WorkflowFactory.prototype.removeWorkflowTask = function(workflow, task, callback) {
  var self = this;
  workflow.removeTask(workflow, task);
  // Update workflow on storage
  return callback(null, workflow);
};

