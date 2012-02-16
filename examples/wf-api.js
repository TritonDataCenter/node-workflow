// Copyright 2012 Pedro P. Candel <kusorbox@gmail.com>. All rights reserved.

// Example of Workflow API usage with custom configuration.

// Run with `node wf-api.js`

var path = require('path'),
    fs = require('fs'),
    restify = require('restify'),
    Logger = require('bunyan'),
    API = require('../lib/api');

var log = new Logger({
  name: 'workflow-api',
  streams: [ {
    level: 'trace',
    stream: process.stdout
  }, {
    level: 'trace',
    path: path.resolve(__dirname, './api.log')
  }],
  serializers: {
    err: Logger.stdSerializers.err,
    req: Logger.stdSerializers.req,
    res: restify.bunyan.serializers.response
  }
});

var config_file = path.normalize(__dirname + '/config.json');
fs.readFile(config_file, 'utf8', function (err, data) {
  if (err) {
    throw err;
  }
  // All vars declaration:
  var config, api, server, port_or_path, backend, Backend;

  config = JSON.parse(data);
  config.logger = log;
  Backend = require(config.backend.module);
  backend = new Backend(config.backend.opts);
  backend.init(function () {
    api = new API(config, backend);
    server = api.server;
    port_or_path = (!config.api.port) ? config.api.path : config.api.port;
    server.listen(port_or_path, function () {
      log.info('%s listening at %s', server.name, server.url);
    });
  });
});
