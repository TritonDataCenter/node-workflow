// Copyright 2012 Pedro P. Candel <kusorbox@gmail.com>. All rights reserved.
//
// Abstract backend, intended to be inherited by real workflow backends
// implementations
var WorkflowBackend = module.exports = function (config) {
  this.config = config;
};

WorkflowBackend.prototype.init = function (callback) {
  callback();
};

WorkflowBackend.prototype.quit = function (callback) {
  callback();
};

// workflow - Workflow object
// callback - f(err, workflow)
WorkflowBackend.prototype.createWorkflow = function (workflow, callback) {
  throw new Error('Backend.createWorkflow is not implemented');
};

// job - Job object
// callback - f(err, job)
WorkflowBackend.prototype.createJob = function (job, callback) {
  throw new Error('Backend.createJob is not implemented');
};

// workflow - Workflow.uuid
WorkflowBackend.prototype.getWorkflow = function (workflow, callback) {
  throw new Error('Backend.getWorkflow is not implemented');
};

// job - Job.uuid
// callback - f(err, job)
WorkflowBackend.prototype.getJob = function (job, callback) {
  throw new Error('Backend.getJob is not implemented');
};

// workflow - Workflow object. Workflow.uuid property used to verify record
//            exists
// callback - f(err, workflow)
WorkflowBackend.prototype.updateWorkflow = function (workflow, callback) {
  throw new Error('Backend.updateWorkflow is not implemented');
};

// Applied only to running jobs, to track progress
// job - Job object. Job.uuid property used to verify record exists.
// callback - f(err, job)
WorkflowBackend.prototype.updateJob = function (job, callback) {
  throw new Error('Backend.updateJob is not implemented');
};

// Update only the given Job property. Intendeed to prevent conflicts with
// two sources updating the same job at the same time, but different properties
// uuid - the job's uuid
// prop - the name of the property to update
// val - value to assign to such property
// callback - f(err) called with error if something fails, otherwise with null.
WorkflowBackend.prototype.updateJobProperty = function (
  uuid,
  prop,
  val,
  callback)
{
  throw new Error('Backend.updateJob is not implemented');
};

// workflow - the workflow object
// callback - f(err, boolean)
WorkflowBackend.prototype.deleteWorkflow = function (workflow, callback) {
  throw new Error('Backend.deleteWorkflow is not implemented');
};


// Get a single job property
// uuid - Job uuid.
// prop - (String) property name
// cb - callback f(err, value)
WorkflowBackend.prototype.getJobProperty = function (uuid, prop, cb) {
  throw new Error('Backend.getJobProperty is not implemented');
};


// job - the job object
// callback - f(err) called with error in case there is a duplicated
// job with the same target and same params
WorkflowBackend.prototype.validateJobTarget = function (job, callback) {
  throw new Error('Backend.validateJobTarget is not implemented');
};

// Job queue related methods:

// Get the next queued job. When there is no next job callback will be called
// with callback(null, null).
// index - Integer, optional. When given, it'll get the job at index position
//         (when not given, it'll return the job at position zero).
// callback - f(err, job)
WorkflowBackend.prototype.nextJob = function (index, callback) {
  throw new Error('Backend.nextJob is not implemented');
};

// Lock a job, mark it as running by the given runner, update job status.
// uuid - the job uuid (String)
// runner_id - the runner identifier (String)
// callback - f(err, job) callback will be called with error if something
//            fails, otherwise it'll return the updated job using getJob.
WorkflowBackend.prototype.runJob = function (uuid, runner_id, callback) {
  throw new Error('Backend.runJob is not implemented');
};

// Unlock the job, mark it as finished, update the status, add the results
// for every job's task.
// job - the job object. It'll be saved to the backend with the provided
//       properties.
// callback - f(err, job) callback will be called with error if something
//            fails, otherwise it'll return the updated job using getJob.
WorkflowBackend.prototype.finishJob = function (job, callback) {
  throw new Error('Backend.finishJob is not implemented');
};

// Queue a job which has been running; i.e, due to whatever the reason,
// re-queue the job. It'll unlock the job, update the status, add the
// results for every finished task so far ...
// job - the job Object. It'll be saved to the backend with the provided
//       properties to ensure job status persistence.
// callback - f(err, job) callback will be called with error if something
//            fails, otherwise it'll return the updated job using getJob.
WorkflowBackend.prototype.queueJob = function (job, callback) {
  throw new Error('Backend.queueJob is not implemented');
};

// Get the given number of queued jobs.
// - start - Integer - Position of the first job to retrieve
// - stop - Integer - Position of the last job to retrieve, _included_
// - callback - f(err, jobs)
WorkflowBackend.prototype.nextJobs = function (start, stop, callback) {
  throw new Error('Backend.nextJobs is not implemented');
};

// Register a runner on the backend and report it's active:
// - runner_id - String, unique identifier for runner.
// - active_at - ISO String timestamp. Optional. If none is given, current time
// - callback - f(err)
WorkflowBackend.prototype.registerRunner = function (
  runner_id,
  active_at,
  callback
) {
  throw new Error('Backend.registerRunner is not implemented');
};

// Report a runner remains active:
// - runner_id - String, unique identifier for runner. Required.
// - active_at - ISO String timestamp. Optional. If none is given, current time
// - callback - f(err)
WorkflowBackend.prototype.runnerActive = function (
  runner_id,
  active_at,
  callback
) {
  throw new Error('Backend.runnerActive is not implemented');
};

// Get the given runner id details
// - runner_id - String, unique identifier for runner. Required.
// - callback - f(err, runner)
WorkflowBackend.prototype.getRunner = function (runner_id, callback) {
  throw new Error('Backend.getRunner is not implemented');
};

// Get all the registered runners:
// - callback - f(err, runners)
WorkflowBackend.prototype.getRunners = function (callback) {
  throw new Error('Backend.getRunners is not implemented');
};

// Set a runner as idle:
// - runner_id - String, unique identifier for runner
// - callback - f(err)
WorkflowBackend.prototype.idleRunner = function (runner_id, callback) {
  throw new Error('Backend.getRunners is not implemented');
};

// Check if the given runner is idle
// - runner_id - String, unique identifier for runner
// - callback - f(boolean)
WorkflowBackend.prototype.isRunnerIdle = function (runner_id, callback) {
  throw new Error('Backend.isRunnerIdle is not implemented');
};

// Remove idleness of the given runner
// - runner_id - String, unique identifier for runner
// - callback - f(err)
WorkflowBackend.prototype.wakeUpRunner = function (runner_id, callback) {
  throw new Error('Backend.isRunnerIdle is not implemented');
};

// Get all jobs associated with the given runner_id
// - runner_id - String, unique identifier for runner
// - callback - f(err, jobs). `jobs` is an array of job's UUIDs.
//   Note `jobs` will be an array, even when empty.
WorkflowBackend.prototype.getRunnerJobs = function (runner_id, callback) {
  throw new Error('Backend.getRunnerJobs is not implemented');
};

// Get all the workflows:
// - callback - f(err, workflows)
WorkflowBackend.prototype.getWorkflows = function (callback) {
  throw new Error('Backend.getWorkflows is not implemented');
};

// Get all the jobs:
// - execution - String, the execution status for the jobs to return.
//               Return all jobs if no execution status is given.
// - callback - f(err, jobs)
WorkflowBackend.prototype.getJobs = function (execution, callback) {
  throw new Error('Backend.getJobs is not implemented');
};

// Add progress information to an existing job:
// - uuid - String, the Job's UUID.
// - info - Object, {'key' => 'Value'}
// - callback - f(err)
WorkflowBackend.prototype.addInfo = function (uuid, info, callback) {
  throw new Error('Backend.addInfo is not implemented');
};

// Get progress information from an existing job:
// - uuid - String, the Job's UUID.
// - callback - f(err, info)
WorkflowBackend.prototype.getInfo = function (uuid, callback) {
  throw new Error('Backend.getInfo is not implemented');
};
