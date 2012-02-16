// Copyright 2012 Pedro P. Candel <kusorbox@gmail.com>. All rights reserved.

// This is shared by api.js and module.js examples

// This is not really needed, but javascriptlint will complain otherwise:
var restify = require('restify');

var workflow = module.exports = {
  name: 'Sample workflow using gist API',
  chain: [ {
    name: 'Post gist',
    timeout: 30,
    retry: 1,
    body: function (job, cb) {
      if (!job.params.login || !job.params.password) {
        return cb('No login/password provided');
      }
      var client = restify.createJsonClient({
        url: 'https://api.github.com'
      });
      client.basicAuth(job.params.login, job.params.password);
      return client.post('/gists', {
        'description': 'a gist created using node-workflow',
        'public': false,
        'files': {
          'example.json': {
            'content': JSON.stringify({
              foo: 'bar',
              bar: 'baz'
            })
          }
        }
      }, function (err, req, res, obj) {
        if (err) {
          return cb(err.name + ': ' + err.body.message);
        } else {
          job.gist_id = obj.id;
          return cb(null, 'Gist created: ' + obj.html_url);
        }
      });
    }
  }, {
    name: 'Get starred gist',
    timeout: 60,
    retry: 2,
    body: function (job, cb) {
      if (!job.params.login || !job.params.password) {
        return cb('No login/password provided');
      }
      if (!job.gist_id) {
        return cb('Unknown gist id');
      }
      var client = restify.createJsonClient({
        url: 'https://api.github.com'
      });
      client.basicAuth(job.params.login, job.params.password);
      return client.get(
        '/gists/' + job.gist_id + '/star',
        function (err, req, res, obj) {
          // Indeed, there should be an error here, which will triger our
          // fallback, where we'll add the star to our gist
          if (err) {
            return cb(err);
          } else {
            // It shouldn't be starred, but ...
            return cb(null, 'What!?, gist was already starred');
          }
        });
    },
    fallback: function (err, job, cb) {
      if (err.statusCode && err.statusCode === 404) {
        // We know how to fix this: star it!:
        var client = restify.createJsonClient({
          url: 'https://api.github.com'
        });
        client.basicAuth(job.params.login, job.params.password);
        return client.put(
          '/gists/' + job.gist_id + '/star', {},
          function (err, req, res, obj) {
            if (err) {
              return cb(err);
            } else {
              return cb(null, 'Gist starred!.');
            }
          });
      } else {
        return cb(err);
      }
    }
  }],
  timeout: 180,
  onerror: [ {
    name: 'Delete gist if exists',
    body: function (job, cb) {
      if (!job.params.login || !job.params.password) {
        return cb('No login/password provided');
      }
      // If we created a gist and something failed later, let's remove it:
      if (job.gist_id) {
        var client = restify.createJsonClient({
          url: 'https://api.github.com'
        });
        client.basicAuth(job.params.login, job.params.password);
        return client.del(
          '/gists/' + job.gist_id,
          function (err, req, res, obj) {
            if (err) {
              return cb(err);
            } else {
              return cb('Cleanup done');
            }
          });
      } else {
        return cb('Nothing to cleanup');
      }
    }
  }]
};
