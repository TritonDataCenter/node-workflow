// Copyright 2012 Pedro P. Candel <kusorbox@gmail.com>. All rights reserved.

module.exports = {
  Factory: function (backend) {
    if (typeof (backend) !== 'object') {
      throw new Error('backend must be an object');
    }

    var WorkflowBackend = require('./workflow-backend'),
        WorkflowFactory = require('./workflow-factory');

    if (!backend.constructor.super_ ||
      backend.constructor.super_ !== WorkflowBackend)
    {
      throw new Error('backend must inherit from WorkflowBackend');
    }

    return new WorkflowFactory(backend);
  },
  Backend: function () {
    var Backend = require('./workflow-in-memory-backend');
    return new Backend();
  },
  API: function (config) {
    if (typeof (config) !== 'object') {
      throw new Error('config must be an object');
    }

    var API = require('./api');
    return new API(config);
  },
  Runner: function (config) {
    if (typeof (config) !== 'object') {
      throw new Error('config must be an object');
    }
    var WorkflowRunner = require('./runner');
    return new WorkflowRunner(config);
  },
  WorkflowBackend: require('./workflow-backend')
};
