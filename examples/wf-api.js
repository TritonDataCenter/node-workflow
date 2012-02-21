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
  // All vars declaration:
  var config, api, server, port_or_path, backend, Backend;

  config = JSON.parse(data);
  Backend = require(config.backend.module);
  backend = new Backend(config.backend.opts);
  backend.init(function () {
    api = new API(config, backend);
    server = api.server;
    port_or_path = (!config.api.port) ? config.api.path : config.api.port;
    server.listen(port_or_path, function () {
      console.log('%s listening at %s', server.name, server.url);
    });
  });
});
