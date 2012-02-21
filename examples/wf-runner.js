// Copyright 2012 Pedro P. Candel <kusorbox@gmail.com>. All rights reserved.

// Example of workflow runner usage with custom configuration.

// Run with `node wf-runner.js`

var path = require('path'),
    fs = require('fs'),
    util = require('util'),
    WorkflowRunner = require('../lib/runner');

var config_file = path.normalize(__dirname + '/config.json');
fs.readFile(config_file, 'utf8', function (err, data) {
  if (err) {
    throw err;
  }
  var config = JSON.parse(data),
      runner = new WorkflowRunner(config);

  runner.init(function (err) {
    if (err) {
      throw err;
    }
    runner.run();
  });
});
