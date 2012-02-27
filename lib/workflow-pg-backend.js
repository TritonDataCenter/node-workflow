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


// job - Job object
// callback - f(err, job)
WorkflowPgBackend.prototype.createJob = function (job, callback) {
  var self = this,
      error = null,
      keys = Object.keys(job),
      idx = 0,
      val_places = [],
      vals = [];

  keys.forEach(function (p) {
    idx += 1;
    if (typeof (job[p]) === 'object') {
      vals.push(JSON.stringify(job[p]));
    } else {
      vals.push(job[p]);
    }
    val_places.push('$' + idx);
  });

  self.client.query(
    'INSERT INTO wf_jobs(' +
    keys.join(', ') + ') VALUES (' +
    val_places.join(', ') + ')', vals).
    on('error', function (err) {
    error = err.Error;
  }).on('end', function () {
    if (error) {
      return callback(error);
    } else {
      return callback(null, job);
    }
  });

};

// uuid - Job.uuid
// callback - f(err, job)
WorkflowPgBackend.prototype.getJob = function (uuid, callback) {
  var self = this,
      error = null,
      job = null;

  self.client.query('SELECT * FROM wf_jobs WHERE uuid=$1', [uuid]).on(
    'error', function (err) {
      error = err;
    }).on('row', function (row) {
      job = self._decodeJob(row);
    }).on('end', function () {
      if (error) {
        return callback(error);
      } else if (job === null) {
        return callback(sprintf(
          'Workflow with uuid \'%s\' does not exist', uuid));
      } else {
        return callback(null, job);
      }
    });
};


// Get a single job property
// uuid - Job uuid.
// prop - (String) property name
// cb - callback f(err, value)
WorkflowPgBackend.prototype.getJobProperty = function (uuid, prop, cb) {
  var self = this,
      error = null,
      value = null,
      encoded_props = ['chain', 'chain_results', 'onerror', 'onerror_results',
      'params'];

  self.client.query(
    'SELECT (' + prop + ') FROM wf_jobs WHERE uuid=$1',
    [uuid]).
  on('error', function (err) {
    error = err.Error;
  }).
  on('row', function (row) {
    if (encoded_props.indexOf(prop) !== -1) {
      value = JSON.parse(row[prop]);
    } else {
      value = row[prop];
    }
  })
  .on('end', function () {
    if (error) {
      return cb(error);
    } else if (value === null) {
      return cb(sprintf(
        'Job with uuid \'%s\' does not exist', uuid));
    } else {
      return cb(null, value);
    }
  });
};

// job - the job object
// callback - f(err) called with error in case there is a duplicated
// job with the same target and same params
WorkflowPgBackend.prototype.validateJobTarget = function (job, callback) {
  var self = this,
      error = null;
  // If no target is given, we don't care:
  if (!job.target) {
    return callback(null);
  }

  return self.client.query('SELECT COUNT(*) FROM wf_jobs WHERE ' +
      'workflow_uuid=$1 AND target=$2 AND params=$3 AND ' +
      'execution=\'queued\' OR execution=\'running\'',
  [job.workflow_uuid, job.target, JSON.stringify(job.params)]).
  on('error', function (err) {
    error = err.Error;
  }).
  on('row', function (row) {
    if (row.count !== 0) {
      error = 'Another job with the same target' +
                ' and params is already queued';
    }
  }).
  on('end', function () {
    if (error) {
      return callback(error);
    } else {
      return callback(null);
    }
  });
};


// Get the next queued job.
// index - Integer, optional. When given, it'll get the job at index position
//         (when not given, it'll return the job at position zero).
// callback - f(err, job)
WorkflowPgBackend.prototype.nextJob = function (index, callback) {
  var self = this,
      error = null,
      job = null;

  if (typeof (index) === 'function') {
    callback = index;
    index = 0;
  }

  self.client.query(
    'SELECT * FROM wf_jobs WHERE execution=\'queued\' ORDER BY created_at ' +
    'ASC LIMIT 1 OFFSET ' + index).
  on('error', function (err) {
    error = err.Error;
  }).
  on('row', function (row) {
    job = self._decodeJob(row);
  }).
  on('end', function () {
    if (error) {
      return callback(error);
    } else {
      return callback(null, job);
    }
  });
};


// Get the given number of queued jobs uuids.
// - start - Integer - Position of the first job to retrieve
// - stop - Integer - Position of the last job to retrieve, _included_
// - callback - f(err, jobs)
WorkflowPgBackend.prototype.nextJobs = function (start, stop, callback) {
  var self = this,
      error = null,
      jobs = [],
      index = start,
      limit = (stop - start) + 1;

  self.client.query(
    'SELECT uuid FROM wf_jobs WHERE execution=\'queued\' ORDER BY created_at ' +
    'ASC LIMIT ' + limit + ' OFFSET ' + index).
  on('error', function (err) {
    error = err.Error;
  }).
  on('row', function (row) {
    jobs.push(row.uuid);
  }).
  on('end', function () {
    if (error) {
      return callback(error);
    } else {
      return callback(null, jobs);
    }
  });
};



// Register a runner on the backend and report it's active:
// - runner_id - String, unique identifier for runner.
// - active_at - ISO String timestamp. Optional. If none is given, current time
// - callback - f(err)
WorkflowPgBackend.prototype.registerRunner = function (
  runner_id,
  active_at,
  callback
) {
  var self = this,
      error = null;

  if (typeof (active_at) === 'function') {
    callback = active_at;
    active_at = new Date().toISOString();
  }

  self.client.query('INSERT INTO wf_runners (uuid, active_at) ' +
    'VALUES ($1, $2)', [runner_id, active_at]).
  on('error', function (err) {
    error = err.Error;
  }).
  on('end', function () {
    if (error) {
      return callback(error);
    } else {
      return callback(null);
    }
  });
};


// Report a runner remains active:
// - runner_id - String, unique identifier for runner. Required.
// - active_at - ISO String timestamp. Optional. If none is given, current time
// - callback - f(err)
WorkflowPgBackend.prototype.runnerActive = function (
  runner_id,
  active_at,
  callback
) {
  var self = this;

  if (typeof (active_at) === 'function') {
    callback = active_at;
    active_at = new Date().toISOString();
  }

  self.client.query('UPDATE wf_runners SET (active_at)=($1) ' +
    'WHERE uuid=$2 RETURNING *', [active_at, runner_id], function (err, res) {
      if (err) {
        return callback(err.Error);
      }
      if (res.rows.length !== 1) {
        return self.registerRunner(runner_id, active_at, callback);
      } else {
        return callback(null);
      }
    });
};

// Get the given runner id details
// - runner_id - String, unique identifier for runner. Required.
// - callback - f(err, runner)
WorkflowPgBackend.prototype.getRunner = function (runner_id, callback) {
  var self = this,
      error = null,
      runner = null;

  self.client.query('SELECT * FROM wf_runners WHERE uuid=$1', [runner_id]).
  on('error', function (err) {
    if (err) {
      error = err.Error;
    }
  }).
  on('row', function (row) {
    runner = row;
  }).
  on('end', function () {
    if (error) {
      return callback(error);
    } else if (runner === null) {
      return callback(sprintf(
        'WorkflowRunner with uuid \'%s\' does not exist', runner_id));
    } else {
      return callback(null, runner);
    }
  });
};


// Get all the registered runners:
// - callback - f(err, runners)
WorkflowPgBackend.prototype.getRunners = function (callback) {
  var self = this,
      error = null,
      runners = [];

  self.client.query('SELECT * FROM wf_runners').
  on('error', function (err) {
    if (err) {
      error = err.Error;
    }
  }).
  on('row', function (row) {
    runners.push(row);
  }).
  on('end', function () {
    if (error) {
      return callback(error);
    } else {
      return callback(null, runners);
    }
  });
};

// Set a runner as idle:
// - runner_id - String, unique identifier for runner
// - callback - f(err)
WorkflowPgBackend.prototype.idleRunner = function (runner_id, callback) {
  var self = this;

  self.client.query('UPDATE wf_runners SET (idle)=(TRUE) ' +
    'WHERE uuid=$1 RETURNING *', [runner_id], function (err, res) {
      if (err) {
        return callback(err.Error);
      }
      if (res.rows.length !== 1) {
        return callback('Cannot idle unexisting runners');
      } else {
        return callback(null);
      }
    });
};

// Check if the given runner is idle
// - runner_id - String, unique identifier for runner
// - callback - f(boolean)
WorkflowPgBackend.prototype.isRunnerIdle = function (runner_id, callback) {
  var self = this,
      error = null,
      is_idle = false;

  self.client.query('SELECT * FROM wf_runners WHERE uuid=$1', [runner_id]).
  on('error', function (err) {
    if (err) {
      error = err.Error;
    }
  }).
  on('row', function (row) {
    is_idle = row.idle;
  }).
  on('end', function () {
    return (error || is_idle) ? callback(true) : callback(false);
  });

};

// Remove idleness of the given runner
// - runner_id - String, unique identifier for runner
// - callback - f(err)
WorkflowPgBackend.prototype.wakeUpRunner = function (runner_id, callback) {
  var self = this;

  self.client.query('UPDATE wf_runners SET (idle)=(FALSE) ' +
    'WHERE uuid=$1 RETURNING *', [runner_id], function (err, res) {
      if (err) {
        return callback(err.Error);
      }
      if (res.rows.length !== 1) {
        return callback('Cannot wake up unexisting runners');
      } else {
        return callback(null);
      }
    });
};

// Get all jobs associated with the given runner_id
// - runner_id - String, unique identifier for runner
// - callback - f(err, jobs). `jobs` is an array of job's UUIDs.
//   Note `jobs` will be an array, even when empty.
WorkflowPgBackend.prototype.getRunnerJobs = function (runner_id, callback) {
  var self = this,
      error = null,
      jobs = [];

  self.client.query(
    'SELECT * FROM wf_jobs WHERE runner_id=$1', [runner_id]).
  on('error', function (err) {
    error = err.Error;
  }).
  on('row', function (row) {
    jobs.push(self._decodeJob(row));
  }).
  on('end', function () {
    if (error) {
      return callback(error);
    } else {
      return callback(null, jobs);
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
    'created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(), ' +
    'runner_id UUID DEFAULT NULL, ' +
    'timeout INTEGER)',
    // wf_runners
    'CREATE ' + temp + 'TABLE wf_runners(' +
    'uuid UUID PRIMARY KEY, ' +
    'active_at TIMESTAMP WITH TIME ZONE, ' +
    'idle BOOLEAN NOT NULL DEFAULT FALSE' +
    ')',
    // indexes
    'CREATE INDEX idx_wf_jobs_execution ON wf_jobs (execution)',
    'CREATE INDEX idx_wf_jobs_target ON wf_jobs (target)',
    'CREATE INDEX idx_wf_jobs_params ON wf_jobs (params)',
    'CREATE INDEX idx_wf_jobs_exec_after ON wf_jobs (exec_after)',
    'CREATE INDEX idx_wf_runners_exec_after ON wf_runners (active_at)',
    'CREATE INDEX idx_wf_runners_runner_id ON wf_jobs (runner_id)'
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

// Return all the JSON.stringified job properties decoded back to objects
// - job - (object) raw job from postgres to decode
WorkflowPgBackend.prototype._decodeJob = function (job) {
  if (job.chain) {
    job.chain = JSON.parse(job.chain);
  }
  if (job.onerror) {
    job.onerror = JSON.parse(job.onerror);
  }
  if (job.chain_results) {
    job.chain_results = JSON.parse(job.chain_results);
  }
  if (job.onerror_results) {
    job.onerror_results = JSON.parse(job.onerror_results);
  }
  if (job.params) {
    job.params = JSON.parse(job.params);
  }
  return job;
};
