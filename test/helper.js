// Copyright 2012 Pedro P. Candel <kusorbox@gmail.com>. All rights reserved.
// Copyright (c) 2017, Joyent, Inc.

var path = require('path'),
    fs = require('fs'),
    existsSync = fs.existsSync || path.existsSync;


var cfg = path.resolve(__dirname, (process.env.TEST_CONFIG_FILE ?
            process.env.TEST_CONFIG_FILE : './config.json'));
var cfg_file = existsSync(cfg) ? cfg :
               path.resolve(__dirname, './config.json.sample'),
               config;

module.exports = {
    config: function () {
        if (!config) {
            config = JSON.parse(fs.readFileSync(cfg_file, 'utf-8'));
        }
        return config;
    }
};
