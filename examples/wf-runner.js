// Copyright 2012 Pedro P. Candel <kusorbox@gmail.com>. All rights reserved.

// Example of workflow runner usage with custom configuration.

// Run with `node wf-runner.js`

var path = require('path'),
    fs = require('fs'),
    util = require('util'),
    Logger = require('bunyan'),
    WorkflowRunner = require('../lib/runner');

var log = new Logger({
  name: 'workflow-runner',
  streams: [ {
    level: 'trace',
    stream: process.stdout
  }, {
    level: 'trace',
    path: path.resolve(__dirname, './runner.log')
  }],
  serializers: {
    err: Logger.stdSerializers.err
  }
});
var config_file = path.normalize(__dirname + '/config.json');
fs.readFile(config_file, 'utf8', function (err, data) {
  if (err) {
    throw err;
  }
  var config, backend, Backend, runner;
  config = JSON.parse(data);
  config.logger = log;
  Backend = require(config.backend.module);
  backend = new Backend(config.backend.opts);
  backend.init(function () {
    runner = new WorkflowRunner(backend, config.runner);
    runner.run();
  });
});
