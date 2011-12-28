// Copyright 2012 Pedro P. Candel <kusorbox@gmail.com>. All rights reserved.
//
// Abstract backend, intended to be inherited by real workflow backends
// implementations
var WorkflowBackend = module.exports = function(config) {
  this.config = config;
};

WorkflowBackend.prototype.init = function(callback) {
  callback();
};

WorkflowBackend.prototype.quit = function(callback) {
  callback();
};

// workflow - Workflow object
// callback - f(err, workflow)
WorkflowBackend.prototype.createWorkflow = function(workflow, callback) {
  throw new Error('Backend.createWorkflow is not implemented');
};
// task - Task object
// callback - f(err, task)
WorkflowBackend.prototype.createTask = function(task, callback) {
  throw new Error('Backend.createTask is not implemented');
};
// job - Job object
// callback - f(err, job)
WorkflowBackend.prototype.createJob = function(job, callback) {
  throw new Error('Backend.createJob is not implemented');
};

// workflow - Workflow.uuid
WorkflowBackend.prototype.getWorkflow = function(workflow, callback) {
  throw new Error('Backend.getWorkflow is not implemented');
};

// task - Task.uuid
// callback - f(err, task)
WorkflowBackend.prototype.getTask = function(task, callback) {
  throw new Error('Backend.getTask is not implemented');
};

// job - Job.uuid
// callback - f(err, job)
WorkflowBackend.prototype.getJob = function(job, callback) {
  throw new Error('Backend.getJob is not implemented');
};

// workflow - Workflow object. Workflow.uuid property used to verify record
//            exists
// callback - f(err, workflow)
WorkflowBackend.prototype.updateWorkflow = function(workflow, callback) {
  throw new Error('Backend.updateWorkflow is not implemented');
};

// task - Task object. Task.uuid property used to verify record exists.
// callback - f(err, task)
WorkflowBackend.prototype.updateTask = function(task, callback) {
  throw new Error('Backend.updateTask is not implemented');
};

// job - Job object. Job.uuid property used to verify record exists.
// callback - f(err, workflow)
WorkflowBackend.prototype.updateJob = function(job, callback) {
  throw new Error('Backend.updateJob is not implemented');
};

// task - the task object
// callback - f(err, boolean)
WorkflowBackend.prototype.deleteTask = function(task, callback) {
  throw new Error('Backend.deleteTask is not implemented');
};

// workflow - the workflow object
// callback - f(err, boolean)
WorkflowBackend.prototype.deleteWorkflow = function(workflow, callback) {
  throw new Error('Backend.deleteWorkflow is not implemented');
};

// job - the job object
// callback - f(err) called with error in case there is a duplicated
// job with the same target and same params
WorkflowBackend.prototype.validateJobTarget = function(job, callback) {
  throw new Error('Backend.validateJobTarget is not implemented');
};
