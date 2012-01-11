// Copyright 2012 Pedro P. Candel <kusorbox@gmail.com>. All rights reserved.
var util = require('util'),
    test = require('tap').test,
    uuid = require('node-uuid'),
    fork = require('hydracp').fork;

test('message without job', function(t) {
  var child = fork(__dirname + '/../lib/runner-child.js', ['some', 'args']);

  child.on('message', function(msg) {
    t.equal(msg.job, '');
    t.equal(msg.error, 'msg.job not present');
  });

  child.on('exit', function(code) {
    t.end();
  });

  child.send({
    nojob: ''
  });

});


test('message with job error', function(t) {
  var aJob = {
    timeout: 3,
    exec_after: '2012-01-03T12:54:05.788Z',
    status: 'running',
    chain_results: [],
    chain: [],
    onerror: []
  },
  task = {
    'uuid': uuid(),
    'name': 'A name',
    'body': function(job, cb) {
      return cb('This will fail');
    }.toString()
  },
  child = fork(__dirname + '/../lib/runner-child.js', ['some', 'args']);

  aJob.chain.push(task);

  child.on('message', function(msg) {
    t.ok(msg.error);
    t.equal(msg.error, 'This will fail');
    t.ok(msg.job);
    t.equal(msg.job.status, 'finished');
    t.equal(msg.job.chain_results[0].error, 'This will fail');
  });

  child.on('exit', function(code) {
    t.end();
  });

  child.send({
    job: aJob
  });
});


test('message with job success', function(t) {
  var aJob = {
    timeout: 3,
    exec_after: '2012-01-03T12:54:05.788Z',
    status: 'running',
    chain_results: [],
    chain: [],
    onerror: []
  },
  task = {
    'uuid': uuid(),
    'name': 'A name',
    'body': function(job, cb) {
      return cb(null);
    }.toString()
  },
  child = fork(__dirname + '/../lib/runner-child.js', ['some', 'args']);

  aJob.chain.push(task);

  child.on('message', function(msg) {
    t.ifError(msg.error);
    t.ok(msg.job);
    t.equal(msg.job.status, 'finished');
    t.equal(msg.job.chain_results[0].result, 'OK');
  });

  child.on('exit', function(code) {
    t.end();
  });

  child.send({
    job: aJob
  });
});


test('message with job queued', function(t) {
  var future = new Date(),
  aJob = {
    timeout: 3,
    status: 'running',
    chain_results: [],
    chain: [],
    onerror: []
  },
  task = {
    'uuid': uuid(),
    'name': 'A name',
    'body': function(job, cb) {
      return cb(null);
    }.toString()
  },
  child = fork(__dirname + '/../lib/runner-child.js', ['some', 'args']);

  future.setTime(new Date().getTime() + 10000);
  aJob.exec_after = future.toISOString();
  aJob.chain.push(task);

  child.on('message', function(msg) {
    t.ifError(msg.error);
    t.ok(msg.job);
    t.equal(msg.job.status, 'queued');
  });

  child.on('exit', function(code) {
    t.end();
  });

  child.send({
    job: aJob
  });
});

