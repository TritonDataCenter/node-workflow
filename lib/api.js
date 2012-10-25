// Copyright 2012 Pedro P. Candel <kusorbox@gmail.com> All rights reserved.
var restify = require('restify'),
    util = require('util'),
    uuid = require('node-uuid'),
    path = require('path'),
    fs = require('fs'),
    vm = require('vm'),
    async = require('async'),
    Logger = require('bunyan'),
    Factory = require('../lib/index').Factory;


var API = module.exports = function (opts) {
    if (typeof (opts) !== 'object') {
        throw new TypeError('opts (Object) required');
    }

    if (typeof (opts.backend) !== 'object') {
        throw new TypeError('opts.backend (Object) required');
    }

    if (typeof (opts.api) !== 'object') {
        throw new TypeError('opts.api (Object) required');
    }

    if (opts.log) {
        this.log = opts.log({
            component: 'workflow-api'
        });
    } else {
        if (!opts.logger) {
            opts.logger = {};
        }

        opts.logger.name = 'workflow-api';
        opts.logger.serializers = {
            err: Logger.stdSerializers.err,
            req: Logger.stdSerializers.req,
            res: restify.bunyan.serializers.response
        };

        opts.logger.streams = opts.logger.streams || [ {
            level: 'info',
            stream: process.stdout
        }];

        this.log = opts.api.Logger = new Logger(opts.logger);
    }

    opts.api.name = opts.api.name || 'WorkflowAPI';
    opts.api.acceptable = ['application/json'];

    if (!opts.api.port && !opts.api.path) {
        opts.api.path = '/tmp/' + uuid();
    }
    this.opts = opts;

    var Backend, factory, backend;
    opts.api.log = this.log;
    this.server = restify.createServer(opts.api);
    Backend = require(opts.backend.module);
    opts.backend.opts.log = this.log;
    this.backend = backend = new Backend(opts.backend.opts);
    factory = Factory(backend);

    // Define path and versioned routes:
    this.WORKFLOWS_PATH = '/workflows';
    this.WORKFLOW_PATH = this.WORKFLOWS_PATH + '/:uuid';
    this.WORKFLOWS_ROUTE = {
        path: this.WORKFLOWS_PATH,
        version: '0.1.0'
    };
    this.WORKFLOW_ROUTE = {
        path: this.WORKFLOW_PATH,
        version: '0.1.0'
    };

    this.JOBS_PATH = '/jobs';
    this.JOB_PATH = this.JOBS_PATH + '/:uuid';
    this.JOBS_ROUTE = {
        path: this.JOBS_PATH,
        version: '0.1.0'
    };
    this.JOB_ROUTE = {
        path: this.JOB_PATH,
        version: '0.1.0'
    };
    this.JOB_INFO_PATH = this.JOB_PATH + '/info';
    this.JOB_INFO_ROUTE = {
        path: this.JOB_INFO_PATH,
        version: '0.1.0'
    };
    this.JOB_CANCEL_PATH = this.JOB_PATH + '/cancel';
    this.JOB_CANCEL_ROUTE = {
        path: this.JOB_CANCEL_PATH,
        version: '0.1.0'
    };


    this.server.use(restify.acceptParser(this.server.acceptable));
    this.server.use(restify.bodyParser());
    this.server.use(restify.queryParser());

    // Define handlers:
    function listWorkflows(req, res, next) {
        backend.getWorkflows(function (err, workflows) {
            if (err) {
                return next(new restify.InternalError(err));
            }
            res.send(200, workflows);
            return next();
        });
    }

    function postWorkflow(req, res, next) {
        var workflow = {},
            wf_members = ['name', 'uuid', 'timeout', 'chain', 'onerror'],
            error,
            meta = {};

        // Up to the user if want to identify the workflow
        // with self.cooked uuid:
        wf_members.forEach(function (p) {
            if (req.params[p]) {
                workflow[p] = req.params[p];
            }
        });

        if (workflow.chain) {
            workflow.chain.forEach(function (task, i, arr) {
                if (!task.body) {
                    error = new restify.ConflictError('Task body is required');
                }
                task.body = vm.runInNewContext('(' + task.body + ')', {});
                if (task.fallback) {
                    task.fallback =
                        vm.runInNewContext('(' + task.fallback + ')', {});
                }
                workflow.chain[i] = task;
            });
        }

        if (workflow.onerror) {
            workflow.onerror.forEach(function (task, i, arr) {
                if (!task.body) {
                    error = new restify.ConflictError('Task body is required');
                }
                task.body = vm.runInNewContext('(' + task.body + ')', {});
                if (task.fallback) {
                    task.fallback =
                        vm.runInNewContext('(' + task.fallback + ')', {});
                }
                workflow.onerror[i] = task;
            });
        }

        if (error) {
            return next(error);
        }

        if (req.headers['x-request-id']) {
            meta.req_id = req.headers['x-request-id'];
        }

        return factory.workflow(workflow, meta, function (err, result) {
            if (err) {
                return next(err.toRestError);
            }
            res.header('Location', req.path + '/' + result.uuid);
            // If X-Request-Id hasn't been set, we'll set it to workflow UUID:
            if (!req.headers['x-request-id']) {
                req.id = res.id = result.uuid;
            }

            res.status(201);
            res.send(result);
            return next();
        });
    }

    function getWorkflow(req, res, next) {
        // If X-Request-Id hasn't been set, we'll set it to workflow UUID:
        if (!req.headers['x-request-id']) {
            req.id = res.id = req.params.uuid;
        }

        var meta = {
            req_id: req.id
        };

        backend.getWorkflow(req.params.uuid, meta, function (err, workflow) {
            if (err) {
                return next(err.toRestError);
            } else {
                res.send(200, workflow);
                return next();
            }
        });
    }

    function updateWorkflow(req, res, next) {
        var error, meta = {};
        // If X-Request-Id hasn't been set, we'll set it to workflow UUID:
        if (!req.headers['x-request-id']) {
            req.id = res.id = req.params.uuid;
        }

        meta.req_id = req.id;

        if (req.params.chain) {
            req.params.chain.forEach(function (task) {
                if (!task.body) {
                    error = new restify.ConflictError('Task body is required');
                }
            });
        }

        if (req.params.onerror) {
            req.params.onerror.forEach(function (task) {
                if (!task.body) {
                    error = new restify.ConflictError('Task body is required');
                }
            });
        }

        if (error) {
            return next(error);
        }

        return backend.updateWorkflow(req.params, meta,
            function (err, workflow) {
                if (err) {
                    return next(err.toRestError);
                }
                res.send(200, workflow);
                return next();
            });
    }

    function deleteWorkflow(req, res, next) {
        // If X-Request-Id hasn't been set, we'll set it to workflow UUID:
        if (!req.headers['x-request-id']) {
            req.id = res.id = req.params.uuid;
        }

        var meta = {
            req_id: req.id
        };

        backend.getWorkflow(req.params.uuid, function (err, workflow) {
            if (err) {
                return next(err.toRestError);
            } else {
                return backend.deleteWorkflow(workflow, meta,
                  function (err, deleted) {
                    if (err) {
                        return next(new restify.InternalError(err));
                    }

                    if (deleted) {
                        res.send(204);
                        return next();
                    } else {
                        return next(new restify.InternalError(
                            'Cannot delete the workflow'));
                    }
                });
            }
        });
    }

    function listJobs(req, res, next) {
        var exec_values =
          ['queued', 'failed', 'succeeded', 'running', 'canceled'],
            cb = function (err, jobs) {
                if (err) {
                    return next(new restify.InternalError(err));
                }
                res.send(200, jobs);
                return next();
            };

        if (req.params.execution) {
            if (exec_values.indexOf(req.params.execution) === -1) {
                return next(new restify.ConflictError(
                  'Execution must be one of queued, failed, ' +
                  'succeeded, canceled or running'));
            }
        }

        if (req.params.offset) {
            req.params.offset = Number(req.params.offset);
        }

        if (req.params.limit) {
            req.params.limit = Number(req.params.limit);
        }

        return backend.getJobs(req.params, cb);
    }

    function postJob(req, res, next) {
        var job = {
            params: {}
        }, meta = {};

        Object.keys(req.params).forEach(function (p) {
            if (['exec_after',
                  'workflow',
                  'target',
                  'uuid'].indexOf(p) !== -1) {
                job[p] = req.params[p];
            } else {
                job.params[p] = req.params[p];
            }
        });

        if (req.headers['x-request-id']) {
            meta.req_id = req.headers['x-request-id'];
        }

        factory.job(job, meta, function (err, result) {
            if (err) {
                if (typeof (err) === 'string') {
                    return next(new restify.ConflictError(err));
                } else {
                    return next(err.toRestError);
                }
            }
            // If X-Request-Id hasn't been set, we'll set it to job UUID:
            if (!req.headers['x-request-id']) {
                req.id = res.id = result.uuid;
            }
            res.header('Location', req.path + '/' + result.uuid);
            res.status(201);
            res.send(result);
            return next();
        });
    }

    function getJob(req, res, next) {
        // If X-Request-Id hasn't been set, we'll set it to job UUID:
        if (!req.headers['x-request-id']) {
            req.id = res.id = req.params.uuid;
        }

        var meta = {
            req_id: req.id
        };

        backend.getJob(req.params.uuid, meta, function (err, job) {
            if (err) {
                return next(err.toRestError);
            } else {
                res.send(200, job);
                return next();
            }
        });
    }

    function getInfo(req, res, next) {
        // If X-Request-Id hasn't been set, we'll set it to job UUID:
        if (!req.headers['x-request-id']) {
            req.id = res.id = req.params.uuid;
        }

        var meta = {
            req_id: req.id
        };

        backend.getInfo(req.params.uuid, meta, function (err, info) {
            if (err) {
                return next(err.toRestError);
            } else {
                res.send(200, info);
                return next();
            }
        });
    }

    function postInfo(req, res, next) {
        var info = {}, meta = {};
        Object.keys(req.params).forEach(function (p) {
            if (p !== 'uuid') {
                info[p] = req.params[p];
            }
        });
        // If X-Request-Id hasn't been set, we'll set it to job UUID:
        if (!req.headers['x-request-id']) {
            req.id = res.id = req.params.uuid;
        }

        meta.req_id = req.id;

        backend.addInfo(req.params.uuid, info, meta, function (err) {
            if (err) {
                return next(err.toRestError);
            } else {
                res.send(200);
                return next();
            }
        });
    }

    function cancelJob(req, res, next) {
        var meta = {};
        backend.getJob(req.params.uuid, function (err, job) {
            if (err) {
                return next(err.toRestError);
            } else if (job.execution === 'succeeded' ||
                            job.execution === 'failed') {
                return next(new restify.ConflictError(
                  'Finished jobs cannot be canceled'));
            } else {
                // If X-Request-Id hasn't been set, we'll set it to job UUID:
                if (!req.headers['x-request-id']) {
                    req.id = res.id = req.params.uuid;
                }
                meta.req_id = req.id;

                return backend.updateJobProperty(
                  job.uuid,
                  'execution',
                  'canceled',
                  meta,
                  function (err) {
                    if (err) {
                        return next(new restify.InternalError(err));
                    }
                    job.execution = 'canceled';
                    res.send(200, job);
                    return next();
                  });
            }
        });
    }
    // --- Routes
    // Workflows:
    this.server.get(this.WORKFLOWS_ROUTE, listWorkflows);
    this.server.head(this.WORKFLOWS_ROUTE, listWorkflows);
    this.server.post(this.WORKFLOWS_ROUTE, postWorkflow);
    // Workflow:
    this.server.get(this.WORKFLOW_ROUTE, getWorkflow);
    this.server.head(this.WORKFLOW_ROUTE, getWorkflow);
    this.server.put(this.WORKFLOW_ROUTE, updateWorkflow);
    this.server.del(this.WORKFLOW_ROUTE, deleteWorkflow);
    // Jobs:
    this.server.get(this.JOBS_ROUTE, listJobs);
    this.server.head(this.JOBS_ROUTE, listJobs);
    this.server.post(this.JOBS_ROUTE, postJob);
    // Job:
    this.server.get(this.JOB_ROUTE, getJob);
    this.server.head(this.JOB_ROUTE, getJob);
    // Cancel job:
    this.server.post(this.JOB_CANCEL_ROUTE, cancelJob);
    // Job status info:
    this.server.get(this.JOB_INFO_ROUTE, getInfo);
    this.server.head(this.JOB_INFO_ROUTE, getInfo);
    this.server.post(this.JOB_INFO_ROUTE, postInfo);
};


API.prototype.init = function (callback) {
    var self = this,
        port_or_path = (!self.opts.api.port) ?
                       self.opts.api.path :
                       self.opts.api.port;
    self.backend.on('error', function (err) {
        return callback(err);
    });

    return self.backend.init(function () {
        self.log.info('API backend initialized');
        self.server.listen(port_or_path, function () {
            self.log.info('%s listening at %s', self.server.name,
                  self.server.url);
            return callback();
        });
    });
};
