// Copyright 2012 Pedro P. Candel <kusorbox@gmail.com>. All rights reserved.

var path = require('path'),
    fs = require('fs');

var cfg = path.resolve(__dirname, './config.json'),
    cfg_file = path.existsSync(cfg) ? cfg :
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
