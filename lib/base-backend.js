var e = require('./errors');
var util = require("util");
var makeEmitter = require("./make-emitter"),
    _ = require('lodash'),
    sprintf = util.format;


/*
 Using Filter with Query Language (ORM)
 http://sailsjs.org/#!/documentation/concepts/ORM/Querylanguage.html
 */

var baseBackend = {
    TYPES: {
        WORKFLOW: 'workflow',
        JOB: 'job',
        RUNNER: 'runner'
    },

    EXECUTION: {
        RUNNING: 'running',
        QUEUED: 'queued',
        CANCELED: 'canceled',
        SUCCEEDED: 'succeeded',
        WAITING: 'waiting',
        FAILED: 'failed',
        RETRIED: 'retried'
    },

    init: function (pCallback) {
        if (pCallback)
            return pCallback();
    },
    // usage: - (test only)
    quit: function (pCallback) {
        if (pCallback)
            return pCallback();
    },
    // usage: internal
    // should save obj to persistence
    // pType - type, TYPES
    // pObj - Object
    // pCallback - f(err, obj)
    save: function (pType, pObj, pCallback) {
        throw new e.BackendInternal('Backend.save() not implemented yet');
    },
    // usage: internal
    // should find object from persistence
    // pType - type, TYPES
    // pFilterObj - Filter for search, e.g. { 'where': { 'attr': 'value' }}
    // pCallback - f(err, objs), objs is an array even if empty
    find: function (pType, pFilterObj, pCallback) {
        throw new e.BackendInternal('Backend.find() not implemented yet');
    },
    // usage: internal
    // should remove object from persistence
    // pType - type, TYPES
    // pObj - Object
    // pCallback - f(err, boolean)
    remove: function (pType, pObj, pCallback) {
        throw new e.BackendInternal('Backend.remove() not implemented yet');
    },
    // usage: Factory
    // workflow - Workflow object
    // meta (optional) - Any additional information to pass to the backend which is not
    //        workflow properties
    // pCallback - f(err, workflow)
    createWorkflow: function (pWorkflow, pMeta, pCallback) {
        if (typeof (pMeta) === 'function') {
            pCallback = pMeta;
            pMeta = {};
        }
        var _self = this;
        this.find(this.TYPES.WORKFLOW, {'name': pWorkflow.name}, function (pError, pWorkflows) {
            if (pWorkflows.length > 0)
                return pCallback(new e.BackendInvalidArgumentError(
                    'Workflow.name must be unique. A workflow with name "' +
                    pWorkflow.name + '" already exists'));
            _self.save(_self.TYPES.WORKFLOW, pWorkflow, pCallback);
        });
    },
    // usage: API & Factory
    // uuid - Workflow.uuid
    // meta (optional) - Any additional information to pass to the backend which is not
    //        workflow properties
    // pCallback - f(err, workflow)
    getWorkflow: function (uuid, meta, pCallback) {
        if (typeof (meta) === 'function') {
            pCallback = meta;
            meta = {};
        }
        this.find(this.TYPES.WORKFLOW, {'uuid': uuid}, function (pError, pWorkflows) {
            if (pWorkflows.length === 0)
                return pCallback(new e.BackendResourceNotFoundError(sprintf(
                    'Workflow with uuid \'%s\' does not exist', uuid)));
            return pCallback(null, pWorkflows[0]);
        });
    },
    // usage: API
    // workflow - the workflow object
    // meta (optional) - Any additional information to pass to the backend which is not
    //        workflow properties
    // pCallback - f(err, boolean)
    deleteWorkflow: function (workflow, meta, pCallback) {
        if (typeof (meta) === 'function') {
            pCallback = meta;
            meta = {};
        }
        this.remove(this.TYPES.WORKFLOW, workflow, pCallback);
    },
    // usage: API
    // workflow - update workflow object.
    // meta (optional) - Any additional information to pass to the backend which is not
    //        workflow properties
    // pCallback - f(err, workflow)
    updateWorkflow: function (workflow, meta, pCallback) {
        if (typeof (meta) === 'function') {
            pCallback = meta;
            meta = {};
        }
        var _self = this;
        this.getWorkflow(workflow.uuid, meta, function (pError, pWorkflow) {
            if (!pWorkflow)
                return pCallback(new e.BackendResourceNotFoundError(
                    'Workflow does not exist. Cannot Update.'));
            _.assign(pWorkflow, workflow);
            _self.find(_self.TYPES.WORKFLOW, {'name': pWorkflow.name}, function (pError, pWorkflows) {
                if (pWorkflows.length > 0)
                    for (var i = 0; i < pWorkflows.length; i++) {
                        if (pWorkflows[i].uuid !== pWorkflow.uuid)
                            return pCallback(new e.BackendInvalidArgumentError(
                                'Workflow.name must be unique. A workflow with name "' +
                                pWorkflow.name + '" already exists'));
                    }
                _self.save(_self.TYPES.WORKFLOW, pWorkflow, pCallback);
            });
        });
    },

    // usage: Factory
    // job - Job object
    // meta (optional) - Any additional information to pass to the backend which is not
    //        job properties
    // pCallback - f(err, job)
    createJob: function (job, meta, pCallback) {
        if (typeof (meta) === 'function') {
            pCallback = meta;
            meta = {};
        }
        job.created_at = job.created_at || new Date().toISOString();
        this.save(this.TYPES.JOB, job, pCallback);
    },

    // usage: Runner
    // uuid - Job.uuid
    // meta (optional) - Any additional information to pass to the backend which is not
    //        job properties
    // pCallback - f(err, job)
    getJob: function (uuid, meta, pCallback) {
        if (typeof (meta) === 'function') {
            pCallback = meta;
            meta = {};
        }
        this.find(this.TYPES.JOB, {'uuid': uuid}, function (pError, pJobs) {
            if (pJobs.length === 0)
                return pCallback(new e.BackendResourceNotFoundError(sprintf(
                    'Job with uuid \'%s\' does not exist', uuid)));
            return pCallback(pError, pJobs[0]);
        });
    },

    // DEPRECATED
    // usage: Internal & Test
    // Get a single job property
    // uuid - Job uuid.
    // prop - (String) property name
    // cb - pCallback f(err, value)
    getJobProperty: function (uuid, prop, pCallback) {
        this.getJob(uuid, function (pError, pJob) {
            if (pError)
                return pCallback(pError);
            if (!pJob)
                return cb(new e.BackendResourceNotFoundError(sprintf(
                    'Job with uuid \'%s\' does not exist', uuid)));
            return pCallback(null, pJob[prop]);
        });
    },

    // usage: Factory
    // job - the job object
    // pCallback - f(err) called with error in case there is a duplicated
    // job with the same target and same params
    validateJobTarget: function (pJob, pCallback) {
        if (typeof (pJob) === 'undefined')
            return pCallback(new e.BackendInternalError('WorkflowRedisBackend.validateJobTarget job(Object) required'));
        // If no target is given, we don't care:
        if (!pJob.target)
            return pCallback(null);
        var _self = this;
        _self.find(_self.TYPES.JOB, null, function (pError, pJobs) {
            if (pError || !pJobs)
                return pCallback(null);
            if (_.some(pJobs, function (job) {
                    if ([_self.EXECUTION.FAILED, _self.EXECUTION.SUCCEEDED].indexOf(job.execution) !== -1 || !job.locks)
                        return false;
                    var re = new RegExp(job.locks);
                    return (re.test(pJob.target));
                })) {
                return pCallback(new e.BackendInvalidArgumentError('Job target is currently locked by another job'));
            }
            var sameTargets = _.where(pJobs, {'target': pJob.target});
            if (sameTargets.length === 0)
                return pCallback(null);
            for (var i = 0; i < sameTargets.length; i++) {
                var job = sameTargets[i];
                if (job.uuid !== pJob.uuid &&
                    job.workflow_uuid === pJob.workflow_uuid &&
                    JSON.stringify(job.params) === JSON.stringify(pJob.params) &&
                    [_self.EXECUTION.QUEUED, _self.EXECUTION.RUNNING, _self.EXECUTION.WAITING].indexOf(job.execution) !== -1)
                    return pCallback(new e.BackendInvalidArgumentError('Another job with the same target and params is already queued'));
            }
            return pCallback(null);
        });
    },

    // usage: - (test-only)
    // Get the next queued job.
    // index (optional) - Integer, optional. When given, it'll get the job at index
    //         position (when not given, it'll return the job at position
    //         zero).
    // pCallback - f(err, job)
    nextJob: function (index, pCallback) {
        if (typeof (index) === 'function') {
            pCallback = index;
            index = 0;
        }
        this.find(this.TYPES.JOB, {
            'where': {'execution': this.EXECUTION.QUEUED},
            'sort': 'created_at'
        }, function (pError, pJobs) {
            return pCallback(null, (pJobs.length === 0 || index >= pJobs.length) ? null :
                pJobs.slice(index, index + 1)[0]);
        });
    },

    // usage: Runner
    // Lock a job, mark it as running by the given runner, update job
    // status.
    // uuid - the job uuid (String)
    // runner_id - the runner identifier (String)
    // pCallback - f(err, job) pCallback will be called with error if
    //            something fails, otherwise it'll return the updated job
    //            using getJob.
    runJob: function (uuid, runner_id, pCallback) {
        var _self = this;
        this.find(this.TYPES.JOB, {'uuid': uuid}, function (pError, pJobs) {
            if (pJobs.length === 0 || pJobs[0].execution != _self.EXECUTION.QUEUED)
                return pCallback(new e.BackendPreconditionFailedError('Only queued jobs can be run'));
            var job = pJobs[0];
            job.runner_id = runner_id;
            job.execution = _self.EXECUTION.RUNNING;
            return _self.save(_self.TYPES.JOB, job, pCallback);
        });
    },

    // usage: Runner
    // Unlock the job, mark it as finished, update the status, add the
    // results for every job's task.
    // job - the job object. It'll be saved to the backend with the provided
    //       properties.
    // pCallback - f(err, job) pCallback will be called with error if
    //            something fails, otherwise it'll return the updated job
    //            using getJob.
    finishJob: function (pJob, pCallback) {
        var _self = this;
        this.find(this.TYPES.JOB, {'uuid': pJob.uuid}, function (pError, pJobs) {
            if (pJobs.length === 0)
                return pCallback(new e.BackendResourceNotFoundError(sprintf(
                    'Job with uuid \'%s\' does not exist', pJob.uuid)));
            var job = pJobs[0];
            if (job.execution !== _self.EXECUTION.RUNNING &&
                job.execution !== _self.EXECUTION.CANCELED)
                return pCallback(new e.BackendPreconditionFailedError(
                    'Only running jobs can be finished'));
            _.assign(job, pJob);
            if (job.execution === _self.EXECUTION.RUNNING)
                job.execution = _self.EXECUTION.SUCCEEDED;
            delete job.runner_id;
            _self.save(_self.TYPES.JOB, job, pCallback);
        });
    },

    // usage: API
    // Update the job while it is running with information regarding
    // progress
    // job - the job object. It'll be saved to the backend with the
    //       provided properties.
    // meta (optional) - Any additional information to pass to the backend which is
    //        not job properties
    // pCallback - f(err, job) pCallback will be called with error if
    //            something fails, otherwise it'll return the updated job
    //            using getJob.
    updateJob: function (pJob, meta, pCallback) {
        if (typeof (meta) === 'function') {
            pCallback = meta;
            meta = {};
        }
        var _self = this;
        this.find(this.TYPES.JOB, {'uuid': pJob.uuid}, function (pError, pJobs) {
            if (pJobs.length === 0)
                return pCallback(new e.BackendResourceNotFoundError(sprintf(
                    'Job with uuid \'%s\' does not exist', pJob.uuid)));
            var job = pJobs[0];
            _.assign(job, pJob);
            _self.save(_self.TYPES.JOB, job, pCallback);
        });
    },

    // usage: Runner
    // Unlock the job, mark it as canceled, and remove the runner_id
    // uuid - string, the job uuid.
    // cb - f(err, job) pCallback will be called with error if something
    //           fails, otherwise it'll return the updated job using getJob.
    cancelJob: function (uuid, pCallback) {
        if (typeof (uuid) === 'undefined') {
            return pCallback(new e.BackendInternalError(
                'cancelJob uuid(String) required'));
        }
        var _self = this;
        this.find(this.TYPES.JOB, {'uuid': uuid}, function (pError, pJobs) {
            if (pJobs.length === 0)
                return pCallback(new e.BackendResourceNotFoundError(sprintf(
                    'Job with uuid \'%s\' does not exist', job.uuid)));
            var job = pJobs[0];
            job.execution = _self.EXECUTION.CANCELED;
            delete job.runner_id;
            _self.save(_self.TYPES.JOB, job, pCallback);
        });
    },

    // DEPRICATED -> use getJob & saveJob
    // usage: API & Runner
    // Update only the given Job property. Intendeed to prevent conflicts
    // with two sources updating the same job at the same time, but
    // different properties:
    // - uuid - the job's uuid
    // - prop - the name of the property to update
    // - val - value to assign to such property
    // - meta (optional) - Any additional information to pass to the backend which is
    //          not job properties
    // - pCallback - f(err) called with error if something fails, otherwise
    //              with null.
    updateJobProperty: function (uuid, prop, val, meta, pCallback) {
        if (typeof (meta) === 'function') {
            pCallback = meta;
            meta = {};
        }
        var _self = this;
        this.find(this.TYPES.JOB, {'uuid': uuid}, function (pError, pJobs) {
            if (pJobs.length === 0)
                return pCallback(new e.BackendResourceNotFoundError(sprintf(
                    'Job with uuid \'%s\' does not exist', uuid)));
            var job = pJobs[0];
            job[prop] = val;
            _self.save(_self.TYPES.JOB, job, pCallback);
        });
    },

    // usage: Runner
    // Queue a job which has been running; i.e, due to whatever the reason,
    // re-queue the job. It'll unlock the job, update the status, add the
    // results for every finished task so far ...
    // job - the job Object. It'll be saved to the backend with the provided
    //       properties to ensure job status persistence.
    // pCallback - f(err, job) pCallback will be called with error if
    //            something fails, otherwise it'll return the updated job
    //            using getJob.
    queueJob: function (pJob, pCallback) {
        var _self = this;
        this.find(this.TYPES.JOB, {'uuid': pJob.uuid}, function (pError, pJobs) {
            if (pJobs.length === 0)
                return pCallback(new e.BackendResourceNotFoundError(sprintf(
                    'Job with uuid \'%s\' does not exist', pJob.uuid)));
            var job = pJobs[0];
            if (job.execution !== _self.EXECUTION.RUNNING)
                return pCallback(new e.BackendPreconditionFailedError(
                    'Only running jobs can be queued again'));
            _.assign(job, pJob);
            delete job.runner_id;
            job.execution = _self.EXECUTION.QUEUED;
            _self.save(_self.TYPES.JOB, job, pCallback);
        });
    },

    // usage: Runner
    // Pause a job which has been running; i.e, tell the job to wait for
    // something external to happen. It'll unlock the job, update the
    // status, add the results for every finished task so far ...
    // job - the job Object. It'll be saved to the backend with the provided
    //       properties to ensure job status persistence.
    // pCallback - f(err, job) pCallback will be called with error if
    //            something fails, otherwise it'll return the updated job
    //            using getJob.
    pauseJob: function (pJob, pCallback) {
        var _self = this;
        this.find(this.TYPES.JOB, {'uuid': pJob.uuid}, function (pError, pJobs) {
            if (pJobs.length === 0)
                return pCallback(new e.BackendResourceNotFoundError(sprintf(
                    'Job with uuid \'%s\' does not exist', pJob.uuid)));
            var job = pJobs[0];
            if (job.execution !== _self.EXECUTION.RUNNING)
                return pCallback(new e.BackendPreconditionFailedError(
                    'Only running jobs can be paused'));
            _.assign(job, pJob);
            delete job.runner_id;
            job.execution = _self.EXECUTION.WAITING;
            _self.save(_self.TYPES.JOB, job, pCallback);
        });
    },

    // usage: - (test only)
    // Tell a waiting job that whatever it has been waiting for has
    // happened and it can run again.
    // job - the job Object. It'll be saved to the backend with the provided
    //       properties to ensure job status persistence.
    // pCallback - f(err, job) pCallback will be called with error if
    //            something fails, otherwise it'll return the updated job
    //            using getJob.
    resumeJob: function (pJob, pCallback) {
        var _self = this;
        this.find(this.TYPES.JOB, {'uuid': pJob.uuid}, function (pError, pJobs) {
            if (pJobs.length === 0)
                return pCallback(new e.BackendResourceNotFoundError(sprintf(
                    'Job with uuid \'%s\' does not exist', pJob.uuid)));
            var job = pJobs[0];
            if (job.execution !== _self.EXECUTION.WAITING)
                return pCallback(new e.BackendPreconditionFailedError(
                    'Only waiting jobs can be resumed'));
            _.assign(job, pJob);
            delete job.runner_id;
            job.execution = _self.EXECUTION.QUEUED;
            _self.save(_self.TYPES.JOB, job, pCallback);
        });
    },

    // usage: Runner
    // Get the given number of queued jobs uuids.
    // - start - Integer - Position of the first job to retrieve
    // - stop - Integer - Position of the last job to retrieve, _included_
    // - pCallback - f(err, jobs). `jobs` is an array of job's UUIDs.
    //   Note `jobs` will be an array, even when empty.
    nextJobs: function (start, stop, pCallback) {
        this.find(this.TYPES.JOB, {
            'where': {'execution': this.EXECUTION.QUEUED},
            'sort': 'created_at'
        }, function (pError, pJobs) {
            if (pJobs.length === 0)
                return pCallback(pError, null);
            var slice = pJobs.slice(start, stop + 1);
            if (slice.length === 0) {
                return pCallback(pError, null);
            } else {
                return pCallback(pError, _.map(slice, 'uuid'));
            }
        });
    },

    // usage: Runner
    // Register a runner on the backend and report it's active:
    // - runner_id - String, unique identifier for runner.
    // - active_at (optional) - ISO String timestamp. Optional. If none is given,
    //   current time
    // - pCallback - f(err)
    registerRunner: function (runner_id, active_at, pCallback) {
        if (typeof (active_at) === 'function') {
            pCallback = active_at;
            active_at = new Date();
        } else if (typeof (active_at) === 'string') {
            active_at = new Date(active_at);
        }
        this.save(this.TYPES.RUNNER, {
            uuid: runner_id,
            active_at: active_at,
            idle: false
        }, function (pError, pRunner) {
            return pCallback(pError);
        });
    },

    // DEPRECATED, use registerRunner
    // usage: Runner
    // Report a runner remains active:
    // - runner_id - String, unique identifier for runner. Required.
    // - active_at (optional) - ISO String timestamp. Optional. If none is given,
    //   current time
    // - pCallback - f(err)
    runnerActive: function (runner_id, active_at, pCallback) {
        return this.registerRunner(runner_id, active_at, pCallback);
    },

    // usage: - (test only)
    // Get the given runner id details
    // - runner_id - String, unique identifier for runner. Required.
    // - pCallback - f(err, runner)
    getRunner: function (runner_id, pCallback) {
        this.find(this.TYPES.RUNNER, {'uuid': runner_id}, function (pError, pRunners) {
            if (pRunners.length === 0)
                return pCallback(new e.BackendResourceNotFoundError(sprintf(
                    'Runner with uuid \'%s\' does not exist', runner_id)));
            return pCallback(pError, pRunners[0].active_at);
        });
    },

    // usage: API & Runner
    // Get all the registered runners:
    // - pCallback - f(err, runners)
    getRunners: function (pCallback) {
        this.find(this.TYPES.RUNNER, null, function (pError, pRunners) {
            var theRunners = {};
            for (var i = 0; i < pRunners.length; i++) {
                var runner = pRunners[i];
                theRunners[runner.uuid] = runner.active_at;
            }
            return pCallback(pError, theRunners);
        });
    },

    // usage: - (test only)
    // Set a runner as idle:
    // - runner_id - String, unique identifier for runner
    // - pCallback - f(err, runner-active_at)
    idleRunner: function (runner_id, pCallback) {
        var _self = this;
        this.find(this.TYPES.RUNNER, {'uuid': runner_id}, function (pError, pRunners) {
            if (pRunners.length === 0)
                return pCallback(new e.BackendResourceNotFoundError(sprintf(
                    'Runner with uuid \'%s\' does not exist', runner_id)));
            var runner = pRunners[0];
            runner.idle = true;
            _self.save(_self.TYPES.RUNNER, runner, function (pError, pRunner) {
                return pCallback(pError, pRunner.active_at);
            });
        });
    },

    // usage: Runner
    // Check if the given runner is idle
    // - runner_id - String, unique identifier for runner
    // - pCallback - f(boolean)
    isRunnerIdle: function (runner_id, pCallback) {
        this.find(this.TYPES.RUNNER, {'uuid': runner_id}, function (pError, pRunners) {
            return pCallback((pRunners.length === 0 || pRunners[0].idle === true));
        });
    },

    // usage: - (test only)
    // Remove idleness of the given runner
    // - runner_id - String, unique identifier for runner
    // - pCallback - f(err)
    wakeUpRunner: function (runner_id, pCallback) {
        var _self = this;
        this.find(this.TYPES.RUNNER, {'uuid': runner_id}, function (pError, pRunners) {
            if (pRunners.length === 0)
                return pCallback(new e.BackendResourceNotFoundError(sprintf(
                    'Runner with uuid \'%s\' does not exist', runner_id)));
            var runner = pRunners[0];
            runner.idle = false;
            _self.save(_self.TYPES.RUNNER, runner, function (pError, pRunner) {
                return pCallback(pError, pRunner.active_at);
            });
        });
    },

    // usage: Runner
    // Get all jobs associated with the given runner_id
    // - runner_id - String, unique identifier for runner
    // - pCallback - f(err, jobs). `jobs` is an array of job's UUIDs.
    //   Note `jobs` will be an array, even when empty.
    getRunnerJobs: function (runner_id, pCallback) {
        this.find(this.TYPES.JOB, {'runner_id': runner_id}, function (pError, pJobs) {
            return pCallback(pError, _.map(pJobs, 'uuid'));
        });
    },

    // usage: API
    // Get all the workflows:
    // - params (optional) - JSON Object (Optional). Can include the value of the
    //  workflow's "name", and any other key/value pair to search for
    //   into workflow's definition.
    // - pCallback - f(err, workflows)
    getWorkflows: function (params, pCallback) {
        if (typeof (params) === 'function') {
            pCallback = params;
            params = {};
        }
        this.find(this.TYPES.WORKFLOW, params, pCallback);
    },

    // usage: API
    // Get all the jobs:
    // - params (optional) - JSON Object. Can include the value of the job's
    //   "execution" status, and any other key/value pair to search for
    //   into job'sparams.
    // - execution - String, the execution status for the jobs to return.
    //                 Return all jobs if no execution status is given.
    // - pCallback - f(err, jobs, count)
    getJobs: function (params, pCallback) {
        if (typeof (params) === 'function') {
            pCallback = params;
            params = {};
        }
        // TODO usage: Waterline Query Language!
        // TODO -> ORM should handle offset & limit
        var offset;
        var limit;
        if (typeof (params) === 'object') {
            offset = params.offset;
            delete params.offset;
            limit = params.limit;
            delete params.limit;
        }
        if (typeof (params) === 'function') {
            pCallback = params;
            params = {};
        }
        var _self = this;
        var executions = Object.keys(this.EXECUTION).map(function (k) {
            return _self.EXECUTION[k]
        });
        if ((typeof (params.execution) !== 'undefined') &&
            (executions.indexOf(params.execution) === -1)) {
            return pCallback(new e.BackendInvalidArgumentError(
                'execution is required and must be one of "' +
                executions.join('", "') + '"'));
        }
        var whereObj = {};
        if (params.execution) {
            whereObj.execution = params.execution;
            delete params.execution;
        }
        this.find(this.TYPES.JOB, whereObj, function (pError, pJobs) {
            if (pError)
                return pCallback(pError, pJobs);
            if (_.keys(params).length > 0)
                pJobs = _.where(pJobs, {'params': params});
            if (typeof (offset) !== 'undefined' &&
                typeof (limit) !== 'undefined') {
                return pCallback(pError, pJobs.slice(offset, limit));
            } else {
                return pCallback(pError, pJobs);
            }
        });
    },

    // usage: API
    // Get count of jobs:
    // - pCallback - f(err, stats)
    countJobs: function (pCallback) {
        var _self = this;
        var executions = Object.keys(this.EXECUTION).map(function (k) {
            return _self.EXECUTION[k]
        });
        var stats = {
            all_time: {},
            past_24h: {},
            past_hour: {},
            current: {}
        };
        executions.forEach(function (e) {
            stats.all_time[e] = 0;
            stats.past_24h[e] = 0;
            stats.past_hour[e] = 0;
            stats.current[e] = 0;
        });
        this.find(this.TYPES.JOB, null, function (pError, pJobs) {
            var yesterday = (function (d) {
                d.setDate(d.getDate() - 1);
                return d;
            })(new Date()).getTime();
            var _1hr = (function (d) {
                d.setHours(d.getHours() - 1);
                return d;
            })(new Date()).getTime();
            var _2hr = (function (d) {
                d.setHours(d.getHours() - 2);
                return d;
            })(new Date()).getTime();
            pJobs = pJobs.map(function (job) {
                return ({
                    execution: job.execution,
                    created_at: new Date(job.created_at).getTime()
                });
            });
            pJobs.forEach(function (j) {
                if (j.created_at < yesterday) {
                    stats.all_time[j.execution] += 1;
                } else if (j.created_at > yesterday && j.created_at < _2hr) {
                    stats.past_24h[j.execution] += 1;
                } else if (j.created_at > _2hr && j.created_at < _1hr) {
                    stats.past_hour[j.execution] += 1;
                } else {
                    stats.current[j.execution] += 1;
                }
            });
            return pCallback(null, stats);
        });
    },

    // usage: API & Runner
    // Add progress information to an existing job:
    // - uuid - String, the Job's UUID.
    // - info - Object, {'key' => 'Value'}
    // - meta (optional) - Any additional information to pass to the backend which is
    //        not job info
    // - pCallback - f(err)
    addInfo: function (uuid, info, meta, pCallback) {
        if (typeof (meta) === 'function') {
            pCallback = meta;
            meta = {};
        }
        var _self = this;
        this.find(this.TYPES.JOB, {'uuid': uuid}, function (pError, pJobs) {
            if (pJobs.length === 0)
                return pCallback(new e.BackendResourceNotFoundError(
                    'Job does not exist. Cannot Update.'));
            var job = pJobs[0];
            if (!util.isArray(job.info)) {
                job.info = [];
            }
            job.info.push(info);
            _self.save(_self.TYPES.JOB, job, pCallback);
        });
    },

    // usage: API
    // Get progress information from an existing job:
    // - uuid - String, the Job's UUID.
    // - meta (optional)
    // - pCallback - f(err, info)
    getInfo: function (uuid, meta, pCallback) {
        if (typeof (meta) === 'function') {
            pCallback = meta;
            meta = {};
        }
        this.find(this.TYPES.JOB, {'uuid': uuid}, function (pError, pJobs) {
            if (pJobs.length === 0)
                return pCallback(new e.BackendResourceNotFoundError(
                    'Job does not exist. Cannot get info.'));
            return pCallback(null, !util.isArray(pJobs[0].info) ? [] : pJobs[0].info);
        });
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