// Copyright 2012 Pedro P. Candel <kusorbox@gmail.com>. All rights reserved.

// This module requires some PostgreSQL 9.1+ features

var util = require('util'),
    async = require('async'),
    pg = require('pg').native,
    WorkflowBackend = require('./workflow-backend');

var sprintf = util.format;

var WorkflowPgBackend = module.exports = function (config) {
  WorkflowBackend.call(this);
  this.config = config;
  this.client = null;
};

util.inherits(WorkflowPgBackend, WorkflowBackend);

// - callback - f(err)
WorkflowPgBackend.prototype.init = function (callback) {
  var self = this,
    pg_opts = {
      port: self.config.port || 5432,
      host: self.config.host || 'localhost',
      database: self.config.database || 'node_workflow',
      user: self.config.user || 'postgres',
      password: self.config.password || ''
    };

  self.client = new pg.Client(pg_opts);

  // TODO: Move this to logger
  self.client.on('notice', function (msg) {
    console.log('notice: %j', msg);
  });

  self.client.connect(function (err) {
    if (err) {
      return callback(err);
    }
    return self._createTables(callback);
  });
};

// Callback - f(err, res);
WorkflowPgBackend.prototype.quit = function (callback) {
  var self = this;
  if (self.client._connected === true) {
    self.client.end();
  }
  callback();
};


// workflow - Workflow object
// callback - f(err, workflow)
WorkflowPgBackend.prototype.createWorkflow = function (workflow, callback) {
  var self = this,
      error = null,
      keys = Object.keys(workflow),
      idx = 0,
      val_places = [],
      vals = [];

  // TODO: A good place to verify that the same tasks are not on the chain
  // and into the onerror callback (GH-1).

  keys.forEach(function (p) {
    idx += 1;
    if (typeof (workflow[p]) === 'object') {
      vals.push(JSON.stringify(workflow[p]));
    } else {
      vals.push(workflow[p]);
    }
    val_places.push('$' + idx);
  });

  self.client.query(
    'INSERT INTO wf_workflows(' +
    keys.join(', ') + ') VALUES (' +
    val_places.join(', ') + ')', vals).
    on('error', function (err) {
    error = (err.code === '23505') ?
            'Workflow.name must be unique. A workflow with name "' +
            workflow.name + '" already exists' :
            err.Error;
  }).on('end', function () {
    if (error) {
      return callback(error);
    } else {
      return callback(null, workflow);
    }
  });
};

// uuid - Workflow.uuid
// callback - f(err, workflow)
WorkflowPgBackend.prototype.getWorkflow = function (uuid, callback) {
  var self = this,
      error = null,
      workflow = null;

  self.client.query('SELECT * FROM wf_workflows WHERE uuid=$1', [uuid]).on(
    'error', function (err) {
      error = err;
    }).on('row', function (row) {
      workflow = {};
      Object.keys(row).forEach(function (p) {
        if (p === 'chain' || p === 'onerror') {
          workflow[p] = JSON.parse(row[p]);
        } else {
          workflow[p] = row[p];
        }
      });
    }).on('end', function () {
      if (error) {
        return callback(error);
      } else if (workflow === null) {
        return callback(sprintf(
          'Workflow with uuid \'%s\' does not exist', uuid));
      } else {
        return callback(null, workflow);
      }
    });
};


// workflow - update workflow object.
// callback - f(err, workflow)
WorkflowPgBackend.prototype.updateWorkflow = function (workflow, callback) {
  var self = this,
      error = null,
      keys = [],
      idx = 0,
      val_places = [],
      vals = [];

  // TODO: A good place to verify that the same tasks are not on the chain
  // and into the onerror callback (GH-1).

  Object.keys(workflow).forEach(function (p) {
    if (p === 'uuid') {
      return;
    }
    idx += 1;
    keys.push(p);
    if (typeof (workflow[p]) === 'object') {
      vals.push(JSON.stringify(workflow[p]));
    } else if (p !== 'uuid') {
      vals.push(workflow[p]);
    }
    val_places.push('$' + idx);
  });

  vals.push(workflow.uuid);
  self.client.query(
    'UPDATE wf_workflows SET (' + keys.join(',') +
    ')=(' + val_places.join(',') + ') WHERE uuid=$' + (idx + 1), vals).
  on('error', function (err) {
    error = err.Error;
  }).on('end', function () {
    if (error) {
      return callback(error);
    } else {
      return callback(null, workflow);
    }
  });
};


// workflow - the workflow object
// callback - f(err, boolean)
WorkflowPgBackend.prototype.deleteWorkflow = function (workflow, callback) {
  var self = this;

  // Use of 'RETURNING *' is a workaround to get the number of deleted rows:
  self.client.query(
    'DELETE FROM wf_workflows WHERE uuid=$1 RETURNING *', [workflow.uuid],
    function (err, result) {
      if (err) {
        return callback(err);
      } else {
        return callback(null, (result.rows.length === 1));
      }
    });
};


// Get all the workflows:
// - callback - f(err, workflows)
WorkflowPgBackend.prototype.getWorkflows = function (callback) {
  var self = this,
      error = null,
      workflows = [];

  self.client.query('SELECT * FROM wf_workflows').
  on('error', function (err) {
    error = err.Error;
  }).
  on('row', function (row) {
    var workflow = {};
    Object.keys(row).forEach(function (p) {
      if (p === 'chain' || p === 'onerror') {
        workflow[p] = JSON.parse(row[p]);
      } else {
        workflow[p] = row[p];
      }
    });
    workflows.push(workflow);
  }).
  on('end', function () {
    if (error) {
      return callback(error);
    } else {
      return callback(null, workflows);
    }
  });
};



// Create required tables when needed:
// - callback - f(err)
WorkflowPgBackend.prototype._createTables = function (callback) {
  var self = this,
  temp = self.config.test ? 'TEMP ' : '',
  queries = [
    // wf_workflows
    'CREATE ' + temp + 'TABLE IF NOT EXISTS wf_workflows(' +
    'name VARCHAR(255) UNIQUE NOT NULL, ' +
    'uuid UUID PRIMARY KEY, ' +
    'chain TEXT,' +
    'onerror TEXT,' +
    'timeout INTEGER)',
    // wf_jobs
    'CREATE ' + temp + 'TABLE IF NOT EXISTS wf_jobs(' +
    'name VARCHAR(255) NOT NULL, ' +
    'uuid UUID PRIMARY KEY, ' +
    'chain TEXT NOT NULL,' +
    'onerror TEXT,' +
    'chain_results TEXT,' +
    'onerror_results TEXT, ' +
    'execution VARCHAR(32) NOT NULL DEFAULT \'queued\' , ' +
    'workflow_uuid UUID NOT NULL, ' +
    'target VARCHAR(255), ' +
    'params TEXT, ' +
    'info TEXT, ' +
    'exec_after TIMESTAMP WITH TIME ZONE, ' +
    'timeout integer)',
    // wf_runners
    'CREATE ' + temp + 'TABLE wf_runners(' +
    'uuid UUID PRIMARY KEY, ' +
    'active_at TIMESTAMP WITH TIME ZONE' +
    ')',
    // indexes
    'CREATE INDEX idx_wf_jobs_execution ON wf_jobs (execution)',
    'CREATE INDEX idx_wf_jobs_target ON wf_jobs (target)',
    'CREATE INDEX idx_wf_jobs_params ON wf_jobs (params)',
    'CREATE INDEX idx_wf_jobs_exec_after ON wf_jobs (exec_after)',
    'CREATE INDEX idx_wf_runners_exec_after ON wf_runners (active_at)'
  ];

  async.forEachSeries(queries, function (query, cb) {
    console.log(query);
    self.client.query(query).on('error', function (err) {
      return cb(err);
    }).on('end', function () {
      return cb();
    });
  }, function (err) {
    if (err) {
      return callback(err);
    }
    return callback();
  });
};
