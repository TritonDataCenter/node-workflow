// Copyright 2012 Pedro P. Candel <kusorbox@gmail.com> All rights reserved.
var restify = require('restify'),
    util = require('util');

var CODES = {
    BackendInternal: 500,
    BackendInvalidArgument: 409,
    BackendMissingParameter: 409,
    BackendPreconditionFailed: 412,
    BackendResourceNotFound: 404
};

function BackendError(code, restCode, message) {
    restify.RestError.call(
      this,
      code,
      restCode || 'BackendError',
      message || 'Backend Error',
      BackendError);

    this.name = 'BackendError';

    this.__defineGetter__('toRestError', function () {
        var errName = this.name.replace(/^Backend/, '');

        if (typeof (restify[errName]) === 'function') {
            return new restify[errName](message);
        } else {
            return new restify.InternalError(message);
        }

    });
}

util.inherits(BackendError, restify.RestError);


module.exports = {
    BackendError: BackendError
};

Object.keys(CODES).forEach(function (k) {
    var name = k + 'Error';
    module.exports[name] = function () {
        var message = util.format.apply(util, arguments);
        BackendError.call(this, CODES[k], k, message);

        this.name = name;
    };
    util.inherits(module.exports[name], BackendError);
});
