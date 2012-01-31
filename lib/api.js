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

  if (typeof(opts.log4js) !== 'object') {
    throw new TypeError('opts.log4js (Object) required');
  }

  if (typeof(opts.api) !== 'object') {
    throw new TypeError('opts.api (Object) required');
  }

  opts.api.name = opts.api.name || 'Workflow API';
  opts.api.acceptable = ['application/json'];
  opts.api.log4js = opts.log4js;

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


  this.server.use(restify.acceptParser(this.server.acceptable));
  this.server.use(restify.bodyParser());

  // Define handlers:
  function listWorkflows(req, res, next) {
    var self = this;
    backend.getWorkflows(function(err, workflows) {
      if (err) {
        return next(new restify.InternalErrorError(err));
      }
      res.send(200, workflows);
      return next();
    });
  }

  function postWorkflow(req, res, next) {
    var self = this,
        chain,
        onerror,
        chain_refs = [],
        onerror_refs = [],
        workflow = {},
        wf_members = ['name', 'uuid', 'timeout'];


    if (req.params.chain) {
      chain = req.params.chain;
      delete req.params.chain;
      workflow.chain = [];
      // If the given chain object is not an array, just override it:
      if (!util.isArray(chain)) {
        chain = [];
      }
    }

    if (req.params.onerror) {
      onerror = req.params.onerror;
      delete req.params.onerror;
      workflow.onerror = [];
      // If the given onerror object is not an array, just override it:
      if (!util.isArray(onerror)) {
        onerror = [];
      }
    }

    // Up to the user if want to identify the workflow with self.cooked uuid:
    wf_members.forEach(function(p) {
      if (req.params[p]) {
        workflow[p] = req.params[p];
      }
    });

    // TODO: If we add /tasks end-point we'll need to check if the provided
    // set of tasks is made of references or tasks objects.

    // Go ahead with tasks chain and onerror, even when the arrays are empty
    // so we keep a single code execution branch:
    async.forEachSeries(chain, function(task, cb) {
      if (!task.body) {
        return cb('Task body is required');
      }
      task.body = vm.runInNewContext('(' + task.body + ')', {});
      if (task.fallback) {
        task.fallback = vm.runInNewContext('(' + task.fallback + ')', {});
      }
      factory.task(task, function(err, result) {
        if (err) {
          return cb(err);
        }
        workflow.chain.push(task.uuid);
        chain_refs.push(task);
        return cb();
      });
    }, function(err) {
      if (err) {
        return next(new restify.ConflictError(err));
      }
      async.forEachSeries(onerror, function(task, cb) {
        task.body = vm.runInNewContext('(' + task.body + ')', {});
        if (task.fallback) {
          task.fallback = vm.runInNewContext('(' + task.fallback + ')', {});
        }
        factory.task(task, function(err, result) {
          if (err) {
            return cb(err);
          }
          workflow.onerror.push(task.uuid);
          onerror_refs.push(task);
          return cb();
        });
      }, function(err) {
        if (err) {
          return next(new restify.ConflictError(err));
        }
        factory.workflow(workflow, function(err, result) {
          if (err) {
            return next(new restify.ConflictError(err));
          }
          res.location = self.WORKFLOWS_PATH + '/' + result.uuid;
          res.status(201);
          res.send(result);
          return next();
        });
      });
    });
  }
  // --- Routes
  // Workflows:
  this.server.get(this.WORKFLOWS_ROUTE, listWorkflows);
  this.server.head(this.WORKFLOWS_ROUTE, listWorkflows);
  this.server.post(this.WORKFLOWS_ROUTE, postWorkflow);
  // Workflow:
  this.server.get(this.WORKFLOW_ROUTE, respond);
  this.server.head(this.WORKFLOW_ROUTE, respond);
  this.server.put(this.WORKFLOW_ROUTE, respond);
  this.server.del(this.WORKFLOW_ROUTE, respond);
  // Jobs:
  this.server.get(this.JOBS_ROUTE, respond);
  this.server.head(this.JOBS_ROUTE, respond);
  this.server.post(this.JOBS_ROUTE, respond);
  // Job:
  this.server.get(this.JOB_ROUTE, respond);
  this.server.head(this.JOB_ROUTE, respond);
  this.server.put(this.JOB_ROUTE, respond);
  this.server.del(this.JOB_ROUTE, respond);
  // Job status info:
  this.server.get(this.JOB_INFO_ROUTE, respond);
  this.server.head(this.JOB_INFO_ROUTE, respond);
  this.server.put(this.JOB_INFO_ROUTE, respond);
};


if (require.main === module) {
  var log4js = require('log4js');
  log4js.addAppender(log4js.fileAppender('logs/api.log'), 'Server');
  // TODO: Make this configurable, wrap into sigusr handler to change log level
  // var logger = log4js.getLogger('Server');
  // logger.setLevel('ERROR');
  var config_file = path.normalize(__dirname + '/../config/config.json');
  fs.readFile(config_file, 'utf8', function(err, data) {
    if (err) {
      throw err;
    }
    // All vars declaration:
    var config, api, server, port_or_path, backend, Backend;

    config = JSON.parse(data);
    config.log4js = log4js;
    Backend = require(config.backend.module);
    backend = new Backend(config.backend.opts);
    backend.init(function() {
      api = new API(config, backend);
      server = api.server;
      port_or_path = (!config.api.port) ? config.api.path : config.api.port;
      server.listen(port_or_path, function() {
        console.log('%s listening at %s', server.name, server.url);
      });
    });
  });
}

