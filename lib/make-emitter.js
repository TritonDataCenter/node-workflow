// Copyright (c) 2016, Joyent, Inc.

var EventEmitter = require('events').EventEmitter;

function makeEmitter(o) {
    var emitter = new EventEmitter();

    o.on = function () {
        return emitter.on.apply(emitter, arguments);
    };

    o.once = function () {
        return emitter.once.apply(emitter, arguments);
    };

    o.addListener = function () {
        return emitter.addListener.apply(emitter, arguments);
    };

    o.emit = function () {
        return emitter.emit.apply(emitter, arguments);
    };

    o.listeners = function () {
        return emitter.listeners.apply(emitter, arguments);
    };

    o.removeAllListeners = function () {
        return emitter.removeAllListeners.apply(emitter, arguments);
    };

    o.removeListener = function () {
        return emitter.removeListener.apply(emitter, arguments);
    };

    o.setMaxListeners = function () {
        return emitter.setMaxListeners.apply(emitter, arguments);
    };

    return o;
}

module.exports = makeEmitter;
