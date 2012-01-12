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

// Job queue related methods:

// Get the next queued job. When there is no next job callback will be called
// with callback(null, null).
// index - Integer, optional. When given, it'll get the job at index position
//         (when not given, it'll return the job at position zero).
// callback - f(err, job)
WorkflowBackend.prototype.nextJob = function(index, callback) {
  throw new Error('Backend.nextJob is not implemented');
};

// Lock a job, mark it as running by the given runner, update job status.
// uuid - the job uuid (String)
// runner_id - the runner identifier (String)
// callback - f(err) callback will be called with error if something fails,
//            otherwise it'll called with null.
WorkflowBackend.prototype.runJob = function(uuid, runner_id, callback) {
  throw new Error('Backend.runJob is not implemented');
};

// Unlock the job, mark it as finished, update the status, add the results
// for every job's task.
// job - the job object. It'll be saved to the backend with the provided
//       properties.
// callback - f(err) called with error if something fails, otherwise with null.
WorkflowBackend.prototype.finishJob = function(job, callback) {
  throw new Error('Backend.finishJob is not implemented');
};

// Queue a job which has been running; i.e, due to whatever the reason,
// re-queue the job. It'll unlock the job, update the status, add the
// results for every finished task so far ...
// job - the job Object. It'll be saved to the backend with the provided
//       properties to ensure job status persistence.
// callback - f(err) same approach, if something fails called with error.
WorkflowBackend.prototype.queueJob = function(job, callback) {
  throw new Error('Backend.queueJob is not implemented');
};

// Get the given number of queued jobs.
// - start - Integer - Position of the first job to retrieve
// - stop - Integer - Position of the last job to retrieve, _included_
// - callback - f(err, jobs)
WorkflowBackend.prototype.nextJobs = function(start, stop, callback) {
  throw new Error('Backend.nextJobs is not implemented');
};

// Register a runner on the backend and report it's active:
// - runner_id - String, unique identifier for runner.
// - callback - f(err)
WorkflowBackend.prototype.registerRunner = function(runner_id, callback) {
  throw new Error('Backend.registerRunner is not implemented');
};

// Report a runner remains active:
// - runner_id - String, unique identifier for runner.
// - callback - f(err)
WorkflowBackend.prototype.runnerActive = function(runner_id, callback) {
  throw new Error('Backend.runnerActive is not implemented');
};

// Get all the registered runners:
// - callback - f(err, runners)
WorkflowBackend.prototype.getRunners = function(callback) {
  throw new Error('Backend.getRunners is not implemented');
};

