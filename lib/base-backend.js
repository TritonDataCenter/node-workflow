var e = require('./errors');
var util = require("util");
var makeEmitter = require("./make-emitter");

var baseBackend = {
    init: function (callback) {
        throw new e.BackendInternal('Backend.init() not implemented yet');
    },
    // usage: - (test only)
    quit: function (callback) {
        throw new e.BackendInternal('Backend.quit() not implemented yet');
    },
    // usage: Factory
    // workflow - Workflow object
    // meta (optional) - Any additional information to pass to the backend which is not
    //        workflow properties
    // callback - f(err, workflow)
    createWorkflow: function (workflow, meta, callback) {
        throw new e.BackendInternal('Backend.createWorkflow() not implemented yet');
    },
    // usage: API & Factory
    // uuid - Workflow.uuid
    // meta (optional) - Any additional information to pass to the backend which is not
    //        workflow properties
    // callback - f(err, workflow)
    getWorkflow: function (uuid, meta, callback) {
        throw new e.BackendInternal('Backend.getWorkflow() not implemented yet');
    },
    // usage: API
    // workflow - the workflow object
    // meta (optional) - Any additional information to pass to the backend which is not
    //        workflow properties
    // callback - f(err, boolean)
    deleteWorkflow: function (workflow, meta, callback) {
        throw new e.BackendInternal('Backend.deleteWorkflow() not implemented yet');
    },
    // usage: API
    // workflow - update workflow object.
    // meta (optional) - Any additional information to pass to the backend which is not
    //        workflow properties
    // callback - f(err, workflow)
    updateWorkflow: function (workflow, meta, callback) {
        throw new e.BackendInternal('Backend.updateWorkflow() not implemented yet');
    },

    // usage: Factory
    // job - Job object
    // meta (optional) - Any additional information to pass to the backend which is not
    //        job properties
    // callback - f(err, job)
    createJob: function (job, meta, callback) {
        throw new e.BackendInternal('Backend.createJob() not implemented yet');
    },

    // usage: Runner
    // uuid - Job.uuid
    // meta (optional) - Any additional information to pass to the backend which is not
    //        job properties
    // callback - f(err, job)
    getJob: function (uuid, meta, callback) {
        throw new e.BackendInternal('Backend.getJob() not implemented yet');
    },

    // usage: Internal & Test
    // Get a single job property
    // uuid - Job uuid.
    // prop - (String) property name
    // cb - callback f(err, value)
    getJobProperty: function (uuid, prop, cb) {
        throw new e.BackendInternal('Backend.getJobProperty() not implemented yet');
    },

    // usage: Factory
    // job - the job object
    // callback - f(err) called with error in case there is a duplicated
    // job with the same target and same params
    validateJobTarget: function (job, callback) {
        throw new e.BackendInternal('Backend.validateJobTarget() not implemented yet');
    },

    // usage: - (test-only)
    // Get the next queued job.
    // index (optional) - Integer, optional. When given, it'll get the job at index
    //         position (when not given, it'll return the job at position
    //         zero).
    // callback - f(err, job)
    nextJob: function (index, callback) {
        throw new e.BackendInternal('Backend.nextJob() not implemented yet');
    },

    // usage: Runner
    // Lock a job, mark it as running by the given runner, update job
    // status.
    // uuid - the job uuid (String)
    // runner_id - the runner identifier (String)
    // callback - f(err, job) callback will be called with error if
    //            something fails, otherwise it'll return the updated job
    //            using getJob.
    runJob: function (uuid, runner_id, callback) {
        throw new e.BackendInternal('Backend.runJob() not implemented yet');
    },

    // usage: Runner
    // Unlock the job, mark it as finished, update the status, add the
    // results for every job's task.
    // job - the job object. It'll be saved to the backend with the provided
    //       properties.
    // callback - f(err, job) callback will be called with error if
    //            something fails, otherwise it'll return the updated job
    //            using getJob.
    finishJob: function (job, callback) {
        throw new e.BackendInternal('Backend.finishJob() not implemented yet');
    },

    // usage: API
    // Update the job while it is running with information regarding
    // progress
    // job - the job object. It'll be saved to the backend with the
    //       provided properties.
    // meta (optional) - Any additional information to pass to the backend which is
    //        not job properties
    // callback - f(err, job) callback will be called with error if
    //            something fails, otherwise it'll return the updated job
    //            using getJob.
    updateJob: function (job, meta, callback) {
        throw new e.BackendInternal('Backend.updateJob() not implemented yet');
    },

    // usage: Runner
    // Unlock the job, mark it as canceled, and remove the runner_id
    // uuid - string, the job uuid.
    // cb - f(err, job) callback will be called with error if something
    //           fails, otherwise it'll return the updated job using getJob.
    cancelJob: function (uuid, cb) {
        throw new e.BackendInternal('Backend.cancelJob() not implemented yet');
    },

    // usage: API & Runner
    // Update only the given Job property. Intendeed to prevent conflicts
    // with two sources updating the same job at the same time, but
    // different properties:
    // - uuid - the job's uuid
    // - prop - the name of the property to update
    // - val - value to assign to such property
    // - meta (optional) - Any additional information to pass to the backend which is
    //          not job properties
    // - callback - f(err) called with error if something fails, otherwise
    //              with null.
    updateJobProperty: function (uuid, prop, val, meta, callback) {
        throw new e.BackendInternal('Backend.updateJobProperty() not implemented yet');
    },

    // usage: Runner
    // Queue a job which has been running; i.e, due to whatever the reason,
    // re-queue the job. It'll unlock the job, update the status, add the
    // results for every finished task so far ...
    // job - the job Object. It'll be saved to the backend with the provided
    //       properties to ensure job status persistence.
    // callback - f(err, job) callback will be called with error if
    //            something fails, otherwise it'll return the updated job
    //            using getJob.
    queueJob: function (job, callback) {
        throw new e.BackendInternal('Backend.queueJob() not implemented yet');
    },

    // usage: Runner
    // Pause a job which has been running; i.e, tell the job to wait for
    // something external to happen. It'll unlock the job, update the
    // status, add the results for every finished task so far ...
    // job - the job Object. It'll be saved to the backend with the provided
    //       properties to ensure job status persistence.
    // callback - f(err, job) callback will be called with error if
    //            something fails, otherwise it'll return the updated job
    //            using getJob.
    pauseJob: function (job, callback) {
        throw new e.BackendInternal('Backend.pauseJob() not implemented yet');
    },

    // usage: - (test only)
    // Tell a waiting job that whatever it has been waiting for has
    // happened and it can run again.
    // job - the job Object. It'll be saved to the backend with the provided
    //       properties to ensure job status persistence.
    // callback - f(err, job) callback will be called with error if
    //            something fails, otherwise it'll return the updated job
    //            using getJob.
    resumeJob: function (job, callback) {
        throw new e.BackendInternal('Backend.resumeJob() not implemented yet');
    },

    // usage: Runner
    // Get the given number of queued jobs uuids.
    // - start - Integer - Position of the first job to retrieve
    // - stop - Integer - Position of the last job to retrieve, _included_
    // - callback - f(err, jobs)
    nextJobs: function (start, stop, callback) {
        throw new e.BackendInternal('Backend.nextJobs() not implemented yet');
    },

    // usage: Runner
    // Register a runner on the backend and report it's active:
    // - runner_id - String, unique identifier for runner.
    // - active_at (optional) - ISO String timestamp. Optional. If none is given,
    //   current time
    // - callback - f(err)
    registerRunner: function (runner_id, active_at, callback) {
        throw new e.BackendInternal('Backend.registerRunner() not implemented yet');
    },

    // usage: Runner
    // Report a runner remains active:
    // - runner_id - String, unique identifier for runner. Required.
    // - active_at (optional) - ISO String timestamp. Optional. If none is given,
    //   current time
    // - callback - f(err)
    runnerActive: function (runner_id, active_at, callback) {
        throw new e.BackendInternal('Backend.runnerActive() not implemented yet');
    },

    // usage: - (test only)
    // Get the given runner id details
    // - runner_id - String, unique identifier for runner. Required.
    // - callback - f(err, runner)
    getRunner: function (runner_id, callback) {
        throw new e.BackendInternal('Backend.getRunner() not implemented yet');
    },

    // usage: API & Runner
    // Get all the registered runners:
    // - callback - f(err, runners)
    getRunners: function (callback) {
        throw new e.BackendInternal('Backend.getRunners() not implemented yet');
    },

    // usage: - (test only)
    // Set a runner as idle:
    // - runner_id - String, unique identifier for runner
    // - callback - f(err)
    idleRunner: function (runner_id, callback) {
        throw new e.BackendInternal('Backend.idleRunner() not implemented yet');
    },

    // usage: Runner
    // Check if the given runner is idle
    // - runner_id - String, unique identifier for runner
    // - callback - f(boolean)
    isRunnerIdle: function (runner_id, callback) {
        throw new e.BackendInternal('Backend.isRunnerIdle() not implemented yet');
    },

    // usage: - (test only)
    // Remove idleness of the given runner
    // - runner_id - String, unique identifier for runner
    // - callback - f(err)
    wakeUpRunner: function (runner_id, callback) {
        throw new e.BackendInternal('Backend.wakeUpRunner() not implemented yet');
    },

    // usage: Runner
    // Get all jobs associated with the given runner_id
    // - runner_id - String, unique identifier for runner
    // - callback - f(err, jobs). `jobs` is an array of job's UUIDs.
    //   Note `jobs` will be an array, even when empty.
    getRunnerJobs: function (runner_id, callback) {
        throw new e.BackendInternal('Backend.getRunnerJobs() not implemented yet');
    },

    // usage: API
    // Get all the workflows:
    // - params (optional) - JSON Object (Optional). Can include the value of the
    //  workflow's "name", and any other key/value pair to search for
    //   into workflow's definition.
    // - callback - f(err, workflows)
    getWorkflows: function (params, callback) {
        throw new e.BackendInternal('Backend.getWorkflows() not implemented yet');
    },

    // usage: API
    // Get all the jobs:
    // - params (optional) - JSON Object. Can include the value of the job's
    //   "execution" status, and any other key/value pair to search for
    //   into job'sparams.
    //   - execution - String, the execution status for the jobs to return.
    //                 Return all jobs if no execution status is given.
    // - callback - f(err, jobs, count)
    getJobs: function (params, callback) {
        throw new e.BackendInternal('Backend.getJobs() not implemented yet');
    },

    // usage: API
    // Get count of jobs:
    // - callback - f(err, stats)
    countJobs: function (callback) {
        throw new e.BackendInternal('Backend.countJobs() not implemented yet');
    },

    // usage: API & Runner
    // Add progress information to an existing job:
    // - uuid - String, the Job's UUID.
    // - info - Object, {'key' => 'Value'}
    // - meta (optional) - Any additional information to pass to the backend which is
    //        not job info
    // - callback - f(err)
    addInfo: function (uuid, info, meta, callback) {
        throw new e.BackendInternal('Backend.addInfo() not implemented yet');
    },

    // usage: API
    // Get progress information from an existing job:
    // - uuid - String, the Job's UUID.
    // - meta (optional)
    // - callback - f(err, info)
    getInfo: function (uuid, meta, callback) {
        throw new e.BackendInternal('Backend.getInfo() not implemented yet');
    }
};

makeEmitter(baseBackend);

module.exports = function (pBackend) {
    var newBackend = {};
    var a;
    for (a in baseBackend) {
        newBackend[a] = baseBackend[a];
    }
    for (a in pBackend) {
        newBackend[a] = pBackend[a];
    }
    return newBackend;
};