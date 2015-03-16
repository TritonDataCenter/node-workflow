// Copyright 2012 Pedro P. Candel <kusorbox@gmail.com>. All rights reserved.

var util = require('util'),
    Logger = require('bunyan'),
    e = require('./errors'),
    _ = require('lodash'),
    clone = require('clone'),
    baseBackend = require('./base-backend');

function MemoryBackend(config) {

    var log;

    if (config.log) {
        log = config.log.child({component: 'wf-in-memory-backend'});
    } else {
        if (!config.logger) {
            config.logger = {};
        }
        config.logger.name = 'wf-in-memory-backend';
        config.logger.serializers = {
            err: Logger.stdSerializers.err
        };
        config.logger.streams = config.logger.streams || [{
            level: 'info',
            stream: process.stdout
        }];
        log = new Logger(config.logger);
    }

    var _store = null;

    var backend = require('./base-backend');
    backend = {
        log: log,
        init: function (callback) {
            _store = {};
            return callback();
        },

        // Never get called, except in test
        quit: function (callback) {
            return callback();
        },

        // usage: internal
        // should save obj to persistence
        // pType - type, TYPES
        // pObj - Object
        // pCallback - f(err, obj)
        save: function (pType, pObj, pCallback) {
            if (!_.has(_store, pType))
                _store[pType] = [];
            // remove old elements
            var item = _.find(_store[pType], {'uuid': pObj.uuid});
            if (item)
                _store[pType] = _.without(_store[pType], item);
            // store new element in array
            _store[pType].push(clone(pObj));
            // call back
            //console.error("save(): %j %j %j", pType, pObj.uuid, _store[pType].length);
            if (pCallback)
                pCallback(null, pObj);
        },
        // usage: internal
        // should find object from persistence
        // pType - type, TYPES
        // pFilterObj - Filter for search, e.g. { 'where': { 'attr': 'value' }}
        // pCallback - f(err, objs), objs is an array even if empty
        find: function (pType, pFilterObj, pCallback) {
            if (!_.has(_store, pType))
                return pCallback(null, []);
            if (!pFilterObj)
                return pCallback(null, clone(_store[pType]));
            var objs = clone(_.where(_store[pType], pFilterObj.where || pFilterObj));
            if (pFilterObj.sort) {
                var sortByArray = pFilterObj.sort.split(' ');
                objs = _.sortBy(objs, sortByArray[0]);
                if (sortByArray.length > 1 && sortByArray[1] == 'DESC')
                    objs.reverse();
            }
            // find & return elements
            //console.error("find(): %j %j %j", pType, pFilterObj, _store[pType].length);
            pCallback(null, objs);
        },
        // usage: internal
        // should remove object from persistence
        // pType - type, TYPES
        // pObj - Object
        // pCallback - f(err, boolean)
        remove: function (pType, pObj, pCallback) {
            if (!_.has(_store, pType))
                return pCallback(null, false);
            var before = _store[pType].length;
            var item = _.find(_store[pType], {'uuid': pObj.uuid});
            if (item) {
                _store[pType] = _.without(_store[pType], item);
                //console.error("remove(): item found & removed: %j", (item !== null));
            }
            //console.error("remove(): %j %j %j", pType, pObj.uuid, _store[pType].length);
            return pCallback(null, before !== _store[pType].length);
        }
    };
    return baseBackend(backend);
}

module.exports = MemoryBackend;