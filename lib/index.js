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
  }
};
