// Copyright 2012 Pedro P. Candel <kusorbox@gmail.com>. All rights reserved.

// Usage example for node-workflow using it as a node module to create
// workflows, queue jobs and obtain the results.

// NOTE it needs `examples/wf-runner.js` running before you run this file.

// Call from parent directory with:
//   `node examples/module.js $login $password`

if (process.argv.length < 4) {
  console.error('Github username and password required as arguments');
  process.exit(1);
}

var $login = process.argv[2],
    $password = process.argv[3];

var util = require('util'),
    assert = require('assert'),
    path = require('path'),
    fs = require('fs'),
    Factory = require('../lib/index').Factory,
    aWorkflow = require('./shared-workflow');

aWorkflow.name = 'a gist created using node-workflow module';

var config_file = path.normalize(__dirname + '/config.json');
fs.readFile(config_file, 'utf8', function (err, data) {
  if (err) {
    throw err;
  }

  var config = JSON.parse(data),
      Backend = require(config.backend.module),
      backend = new Backend(config.backend.opts),
      factory;

  backend.init(function () {
    factory = Factory(backend);
    factory.workflow(aWorkflow, function (err, wf) {
      assert.ifError(err);
      assert.ok(wf);
      var aJob = {
        target: '/gists',
        workflow: wf.uuid,
        params: {
          login: $login,
          password: $password
        }
      };
      factory.job(aJob, function (err, job) {
        assert.ifError(err);
        assert.ok(job);
        assert.equal(job.execution, 'queued');
        assert.ok(job.uuid);

        var intervalId = setInterval(function () {
          backend.getJob(job.uuid, function (err, obj) {
            assert.ifError(err);
            if (obj.execution === 'queued') {
              console.log('Job waiting to be processed');
            } else if (obj.execution === 'running') {
              console.log('Job in progress ...');
            } else {
              console.log('Job finished. Here come the results:');
              console.log(util.inspect(obj, false, 8));
              // Only one workflow with the same name, need to delete it
              // to allow creating it again:
              backend.deleteWorkflow(wf, function (err, res) {
                assert.ifError(err);
                assert.ok(res);
                clearInterval(intervalId);
                process.exit(0);
              });
            }
          });
        }, 3000);
      });
    });
  });
});
