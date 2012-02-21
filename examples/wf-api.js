// Copyright 2012 Pedro P. Candel <kusorbox@gmail.com>. All rights reserved.

// Example of Workflow API usage with custom configuration.

// Run with `node wf-api.js`

var path = require('path'),
    fs = require('fs'),
    API = require('../lib/api');

var config_file = path.normalize(__dirname + '/config.json');
fs.readFile(config_file, 'utf8', function (err, data) {
  if (err) {
    throw err;
  }

  var config = JSON.parse(data),
      api = new API(config);

  api.init(function () {
    console.log('API server up and running!');
  });
});
