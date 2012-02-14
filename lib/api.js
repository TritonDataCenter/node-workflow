// Copyright 2012 Pedro P. Candel <kusorbox@gmail.com> All rights reserved.
var restify = require('restify'),
    util = require('util'),
    uuid = require('node-uuid'),
    path = require('path'),
    fs = require('fs'),
    vm = require('vm'),
    async = require('async'),
    Factory = require('../lib/index').Factory;

function respond(req, res, next) {
  res.send({
    hello: req.params.uuid
  });
}

var API = module.exports = function(opts, backend) {
  if (typeof(opts) !== 'object') {
    throw new TypeError('opts (Object) required');
  }

  if (typeof(backend) !== 'object') {
    throw new TypeError('backend (Object) required');
  }

  if (typeof(opts.logger) !== 'object') {
    throw new TypeError('opts.logger (Object) required');
  }

  if (typeof(opts.api) !== 'object') {
    throw new TypeError('opts.api (Object) required');
  }

  opts.api.name = opts.api.name || 'Workflow API';
  opts.api.acceptable = ['application/json'];
  opts.api.Logger = opts.logger;

  if (!opts.api.port && !opts.api.path) {
    opts.api.path = '/tmp/' + uuid();
  }

  this.server = restify.createServer(opts.api);
  this.backend = backend;
  var factory = Factory(backend);

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
    backend.getWorkflows(function(err, workflows) {
      if (err) {
        return next(new restify.InternalErrorError(err));
      }
      res.send(200, workflows);
      return next();
    });
  }

  function postWorkflow(req, res, next) {
    var workflow = {},
        wf_members = ['name', 'uuid', 'timeout', 'chain', 'onerror'],
        error;

    // Up to the user if want to identify the workflow with self.cooked uuid:
    wf_members.forEach(function(p) {
      if (req.params[p]) {
        workflow[p] = req.params[p];
      }
    });

    if (workflow.chain) {
      workflow.chain.forEach(function(task, i, arr) {
        if (!task.body) {
          error = new restify.ConflictError('Task body is required');
        }
        task.body = vm.runInNewContext('(' + task.body + ')', {});
        if (task.fallback) {
          task.fallback = vm.runInNewContext('(' + task.fallback + ')', {});
        }
        workflow.chain[i] = task;
      });
    }

    if (workflow.onerror) {
      workflow.onerror.forEach(function(task, i, arr) {
        if (!task.body) {
          error = new restify.ConflictError('Task body is required');
        }
        task.body = vm.runInNewContext('(' + task.body + ')', {});
        if (task.fallback) {
          task.fallback = vm.runInNewContext('(' + task.fallback + ')', {});
        }
        workflow.onerror[i] = task;
      });
    }

    if (error) {
      return next(error);
    }

    factory.workflow(workflow, function(err, result) {
      if (err) {
        return next(new restify.ConflictError(err));
      }
      res.header('Location', req.path + '/' + result.uuid);
      res.status(201);
      res.send(result);
      return next();
    });
  }

  function getWorkflow(req, res, next) {
    backend.getWorkflow(req.params.uuid, function(err, workflow) {
      if (err) {
        return next(new restify.InternalErrorError(err));
      }
      // workflow = {}
      if (Object.keys(workflow).length === 0) {
        return next(new restify.ResourceNotFoundError(
            'Workflow ' + req.params.uuid + ' not found'
        ));
      } else {
        res.send(200, workflow);
        return next();
      }
    });
  }

  function updateWorkflow(req, res, next) {
    var error;

    if (req.params.chain) {
      req.params.chain.forEach(function(task) {
        if (!task.body) {
          error = new restify.ConflictError('Task body is required');
        }
      });
    }

    if (req.params.onerror) {
      req.params.onerror.forEach(function(task) {
        if (!task.body) {
          error = new restify.ConflictError('Task body is required');
        }
      });
    }

    if (error) {
      return next(error);
    }

    backend.updateWorkflow(req.params, function(err, workflow) {
      if (err) {
        if (err === 'Workflow does not exist. Cannot Update.') {
          return next(new restify.ResourceNotFoundError(
            'Workflow ' + req.params.uuid + ' not found'
          ));
        } else {
          return next(new restify.InternalErrorError(err));
        }
      }
      res.send(200, workflow);
      return next();
    });
  }

  function deleteWorkflow(req, res, next) {
    backend.getWorkflow(req.params.uuid, function(err, workflow) {
      if (err) {
        return next(new restify.InternalErrorError(err));
      }
      // workflow = {}
      if (Object.keys(workflow).length === 0) {
        return next(new restify.ResourceNotFoundError(
            'Workflow ' + req.params.uuid + ' not found'
        ));
      } else {
        backend.deleteWorkflow(workflow, function(err, deleted) {
          if (err) {
            return next(new restify.InternalErrorError(err));
          }

          if (deleted) {
            res.send(204);
            return next();
          } else {
            return next(new restify.InternalErrorError(
                'Cannot delete the workflow'
            ));
          }
        });
      }
    });
  }

  function listJobs(req, res, next) {
    var exec_values =
      ['queued', 'failed', 'succeeded', 'running', 'canceled'],
        cb = function(err, jobs) {
          if (err) {
            return next(new restify.InternalErrorError(err));
          }
          res.send(200, jobs);
          return next();
        };

    if (req.params.execution) {
      if (exec_values.indexOf(req.params.execution) === -1) {
        return next(new restify.ConflictError(
          'Execution must be one of queued, failed, ' +
          'succeeded, canceled or running'
        ));
      } else {
        backend.getJobs(req.params.execution, cb);
      }
    } else {
      backend.getJobs(cb);
    }
  }

  function postJob(req, res, next) {
    var job = {
      params: {}
    };

    Object.keys(req.params).forEach(function(p) {
      if (['exec_after', 'workflow', 'target', 'uuid'].indexOf(p) !== -1) {
        job[p] = req.params[p];
      } else {
        job.params[p] = req.params[p];
      }
    });

    factory.job(job, function(err, result) {
      if (err) {
        return next(new restify.ConflictError(err));
      }
      res.header('Location', req.path + '/' + result.uuid);
      res.status(201);
      res.send(result);
      return next();
    });
  }

  function getJob(req, res, next) {
    backend.getJob(req.params.uuid, function(err, job) {
      if (err) {
        return next(new restify.InternalErrorError(err));
      }
      // workflow = {}
      if (Object.keys(job).length === 0) {
        return next(new restify.ResourceNotFoundError(
            'Job ' + req.params.uuid + ' not found'
        ));
      } else {
        res.send(200, job);
        return next();
      }
    });
  }

  function getInfo(req, res, next) {
    backend.getInfo(req.params.uuid, function(err, info) {
      if (err) {
        if (err === 'Job does not exist. Cannot get info.') {
          return next(new restify.ResourceNotFoundError(err));
        } else {
          return next(new restify.InternalErrorError(err));
        }
      } else {
        res.send(200, info);
        return next();
      }
    });
  }

  function postInfo(req, res, next) {
    var info = {};
    Object.keys(req.params).forEach(function(p) {
      if (p !== 'uuid') {
        info[p] = req.params[p];
      }
    });
    backend.addInfo(req.params.uuid, info, function(err) {
      if (err) {
        if (err === 'Job does not exist. Cannot Update.') {
          return next(new restify.ResourceNotFoundError(err));
        } else {
          return next(new restify.ConflictError(err));
        }
      } else {
        res.send(200);
        return next();
      }
    });
  }

  function cancelJob(req, res, next) {
    backend.getJob(req.params.uuid, function(err, job) {
      if (err) {
        return next(new restify.InternalErrorError(err));
      }
      // workflow = {}
      if (Object.keys(job).length === 0) {
        return next(new restify.ResourceNotFoundError(
            'Job ' + req.params.uuid + ' not found'
        ));
      } else if (job.execution === 'succeeded' || job.execution === 'failed') {
        return next(new restify.ConflictError(
          'Finished jobs cannot be canceled'
        ));
      } else {
        backend.updateJobProperty(
          job.uuid,
          'execution',
          'canceled',
          function(err) {
            if (err) {
              return next(new restify.InternalErrorError(err));
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


if (require.main === module) {
  var Logger = require('bunyan');
  var log = new Logger({
    name: 'workflow-api',
    streams: [{
      level: 'debug',
      stream: process.stdout
    }, {
      level: 'debug',
      path: path.resolve(__dirname, '../logs/api.log')
    }],
    serializers: {
      err: Logger.stdSerializers.err,
      req: Logger.stdSerializers.req,
      res: restify.bunyan.serializers.response
    }
  });

  // TODO: Make this configurable, wrap into sigusr handler to change log level
  var config_file = path.normalize(__dirname + '/../config/config.json');
  fs.readFile(config_file, 'utf8', function(err, data) {
    if (err) {
      throw err;
    }
    // All vars declaration:
    var config, api, server, port_or_path, backend, Backend;

    config = JSON.parse(data);
    config.logger = log;
    Backend = require(config.backend.module);
    backend = new Backend(config.backend.opts);
    backend.init(function() {
      api = new API(config, backend);
      server = api.server;
      port_or_path = (!config.api.port) ? config.api.path : config.api.port;
      server.listen(port_or_path, function() {
        log.info('%s listening at %s', server.name, server.url);
      });
    });
  });
}

