// Copyright 2012 Pedro P. Candel <kusorbox@gmail.com>. All rights reserved.

// This is an example of the same gist API requests made straight from NodeJS
// without using node-workflow, so differences can be quickly stablished with
// the way to proceed to write workflow tasks

if (process.argv.length < 4) {
  console.error('Github username and password required as arguments');
  process.exit(1);
}

var $login = process.argv[2],
    $password = process.argv[3];

var restify = require('restify'),
    util = require('util');

var client = restify.createJsonClient({
  url: 'https://api.github.com'
});

client.basicAuth($login, $password);

client.post('/gists', {
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
    console.error(err.name + ': ' + err.body.message);
    if (err.statusCode === 401) {
      process.exit(1);
    }
  } else {
    // res.statusCode === 201
    // res.location === obj.url
    // obj.url === client.url + req.path + '/' + obj.id
    var gist = obj;
    console.info('Gist object with id ' + gist.id + ' created');
    client.get('/gists/' + gist.id + '/star', function (err, req, res, obj) {
      console.log(util.inspect(err, false, 8));
      console.log(util.inspect(obj, false, 8));
      if (err) {
        if (err.statusCode === 404) {
          client.put(
            '/gists/' + gist.id + '/star', {},
            function (err, req, res, obj) {
              if (err) {
                console.error(err.name + ': ' + err.body.message);
                process.exit(1);
              } else {
                console.info('Gist is starred: ' + gist.html_url);
                process.exit(0);
              }
            });
        } else {
          console.error(err.name + ': ' + err.body.message);
          process.exit(1);
        }
      } else {
        console.info('Gist is starred: ' + gist.html_url);
        process.exit(0);
      }
    });
  }
});
