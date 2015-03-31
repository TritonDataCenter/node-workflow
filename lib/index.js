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

        return WorkflowFactory(backend);
    },
    MemoryBackend: function (config) {
        var MemoryBackend = require('./workflow-in-memory-backend');
        return MemoryBackend(config);
    },
    BaseBackend: function (config) {
        return require('./base-backend')(config);
    },
    API: function (config) {
        if (typeof (config) !== 'object') {
            throw new Error('config must be an object');
        }
        var API = require('./api');
        return API(config);
    },
    Runner: function (config) {
        if (typeof (config) !== 'object') {
            throw new Error('config must be an object');
        }
        var WorkflowRunner = require('./runner');
        config.dtrace = createDTrace('workflow');
        return WorkflowRunner(config);
    },
    CreateDTrace: createDTrace,
    makeEmitter: require('./make-emitter')
};

var errors = require('./errors');
Object.keys(errors).forEach(function (k) {
    module.exports[k] = errors[k];
});
