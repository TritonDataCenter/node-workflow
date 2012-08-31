// Copyright 2012 Pedro P. Candel <kusorbox@gmail.com>. All rights reserved.

module.exports = {
    Factory: function (backend) {
        if (typeof (backend) !== 'object') {
            throw new Error('backend must be an object');
        }

        var WorkflowFactory = require('./workflow-factory');

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

var errors = require('./errors');
Object.keys(errors).forEach(function (k) {
    module.exports[k] = errors[k];
});
