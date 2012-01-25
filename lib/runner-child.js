// Copyright 2012 Pedro P. Candel <kusorbox@gmail.com> All rights reserved.
//
// Child process to be "forked" from runner.js using hydracp.
//
var util = require('util'),
    Workflow = require('./workflow');

// Every possible situation finishes this way, notifying parent process about
// execution results:
function notifyParent(err, job) {
  process.send({
    error: err,
    job: job
  });

  setTimeout(function() {
    process.exit();
  }, 500);
}

// Just sent parent a message without exiting the process.
// (Used as notifier for workflow.run, to persist information about finished
// tasks).
function reportProgress(job) {
  process.send({
    error: '',
    job: job
  });
}

// This is the only way we have for communication from the parent process.
process.on('message', function(msg) {
  if (!msg.job) {
    return notifyParent('msg.job not present', '');
  }

  var curDate = new Date(),
      workflow = new Workflow(msg.job);

  // May need to re-queue the job:
  if (workflow.exec_after > curDate) {
    workflow.job.execution = 'queued';
    return notifyParent('', workflow.job);
  }

  workflow.run(reportProgress, function(err) {
    return notifyParent(((err) ? err : ''), workflow.job);
  });
});

