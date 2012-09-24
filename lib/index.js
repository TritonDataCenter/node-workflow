// Copyright 2012 Pedro P. Candel <kusorbox@gmail.com>. All rights reserved.

var DTRACE;

// Shamelessly copied from https://github.com/mcavage/node-restify
function createDTrace(name) {
    // see https://github.com/mcavage/node-restify/issues/80 and
    // https://github.com/mcavage/node-restify/issues/100
    if (!DTRACE) {
        try {
            var d = require('dtrace-provider');
            DTRACE = d.createDTraceProvider(name);
        } catch (e) {
            DTRACE = {
                addProbe: function addProbe() {},
                enable: function enable() {},
                fire: function fire() {}
            };
        }
    }
    return (DTRACE);
}

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
        config.dtrace = createDTrace('workflow');
        return new WorkflowRunner(config);
    },
    WorkflowBackend: require('./workflow-backend'),
    CreateDTrace: createDTrace
};

var errors = require('./errors');
Object.keys(errors).forEach(function (k) {
    module.exports[k] = errors[k];
});
