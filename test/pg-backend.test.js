// Copyright 2012 Pedro P. Candel <kusorbox@gmail.com>. All rights reserved.

// Note the following database will not be created, and needs to exist before
// the tests can successfully run:
//
//    `create database node_workflow_test owner postgres;`
//
// or whatever the postgres user you want to run tests as.

var test = require('tap').test,
    uuid = require('node-uuid'),
    SOCKET = '/tmp/.' + uuid(),
    util = require('util'),
    async = require('async'),
    Factory = require('../lib/index').Factory,
    WorkflowPgBackend = require('../lib/workflow-pg-backend');

var backend, factory;

var aWorkflow, aJob, anotherJob;

var helper = require('./helper'),
    config = helper.config(),
    runnerId = config.runner.identifier;

var pg_opts = {
  database: 'node_workflow_test',
  test: true
};

test('setup', function (t) {
  backend = new WorkflowPgBackend(pg_opts);
  t.ok(backend, 'backend ok');
  backend.init(function (err) {
    t.ifError(err, 'backend init error');
    factory = Factory(backend);
    t.ok(factory, 'factory ok');
    t.end();
  });
});


test('add a workflow', function (t) {
  factory.workflow({
    name: 'A workflow',
    chain: [ {
      name: 'A Task',
      timeout: 30,
      retry: 3,
      body: function (job, cb) {
        return cb(null);
      }
    }],
    timeout: 180,
    onError: [ {
      name: 'Fallback task',
      body: function (job, cb) {
        return cb('Workflow error');
      }
    }]
  }, function (err, workflow) {
    t.ifError(err, 'add workflow error');
    t.ok(workflow, 'add workflow ok');
    aWorkflow = workflow;
    t.ok(workflow.chain[0].uuid, 'add workflow chain task');
    t.ok(workflow.onerror[0].uuid, 'add workflow onerror task');
    t.end();
  });
});


test('workflow name must be unique', function (t) {
  factory.workflow({
    name: 'A workflow',
    chain: [ {
      name: 'A Task',
      timeout: 30,
      retry: 3,
      body: function (job, cb) {
        return cb(null);
      }
    }],
    timeout: 180,
    onError: [ {
      name: 'Fallback task',
      body: function (job, cb) {
        return cb('Workflow error');
      }
    }]
  }, function (err, workflow) {
    t.ok(err, 'duplicated workflow name err');
    t.end();
  });
});


test('get workflow', function (t) {
  backend.getWorkflow(aWorkflow.uuid, function (err, workflow) {
    t.ifError(err, 'get workflow error');
    t.ok(workflow, 'get workflow ok');
    t.equivalent(workflow, aWorkflow);
    backend.getWorkflow(uuid(), function (err, workflow) {
      t.ok(err.match(/uuid/gi), 'unexisting workflow error');
      t.end();
    });
  });
});


test('update workflow', function (t) {
  aWorkflow.chain.push({
    name: 'Another task',
    body: function (job, cb) {
      return cb(null);
    }.toString()
  });
  aWorkflow.name = 'A workflow name';
  backend.updateWorkflow(aWorkflow, function (err, workflow) {
    t.ifError(err, 'update workflow error');
    t.ok(workflow, 'update workflow ok');
    t.ok(workflow.chain[1].name, 'Updated task ok');
    t.ok(workflow.chain[1].body, 'Updated task body ok');
    t.end();
  });
});




test('get workflows', function (t) {
  backend.getWorkflows(function (err, workflows) {
    t.ifError(err, 'get workflows error');
    t.ok(workflows, 'workflows ok');
    t.equal(workflows[0].uuid, aWorkflow.uuid, 'workflow uuid ok');
    t.ok(util.isArray(workflows[0].chain), 'workflow chain ok');
    t.ok(util.isArray(workflows[0].onerror), 'workflow onerror ok');
    t.end();
  });
});


test('delete workflow', function (t) {
  t.test('when the workflow exists', function (t) {
    backend.deleteWorkflow(aWorkflow, function (err, success) {
      t.ifError(err, 'delete existing workflow error');
      t.ok(success);
      t.end();
    });
  });
  t.test('when the workflow does not exist', function (t) {
    backend.deleteWorkflow(aWorkflow, function (err, success) {
      t.ifError(err, 'delete unexisting workflow error');
      t.equal(success, false, 'no row deleted');
      t.end();
    });
  });
  t.end();
});


test('teardown', function (t) {
  backend.client.query('DROP TABLE wf_workflows', function (err, res) {
    t.ifError(err, 'drop wf_workflows error');
    backend.client.query('DROP TABLE wf_jobs', function (err, res) {
      t.ifError(err, 'drop wf_jobs error');
      backend.quit(function () {
        t.end();
      });
    });
  });
});
