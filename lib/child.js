// Copyright 2012 Pedro P. Candel <kusorbox@gmail.com> All rights reserved.
//
// Child process to be "forked" from task-runner.js.
//
var util = require('util'),
    WorkflowTaskRunner = require('./task-runner'),
    wf_task_runner;

// Every possible situation finishes this way, notifying parent process about
// execution results:
function notifyParent(msg) {
    process.send(msg);
}



// This is the only way we have for communication from the parent process.
//
// Main case:
// - We receive a message from parent including the 'task' to run and the 'job'
//   object itself. Optionally, this object may also contain a 'sandbox' object
//   and 'trace' enabled.
// Side case:
// - We receive a message to finish the task "as is" due to a "finish task now"
//   call.
process.on('message', function (msg) {
    if (msg.job && msg.task) {
        try {
            wf_task_runner = new WorkflowTaskRunner(msg);
            wf_task_runner.runTask(notifyParent);
        } catch (e) {
            notifyParent({
                error: e.message
            });
        }
    } else if (msg.cmd && msg.cmd === 'cancel') {
        // Cancel message received from job runner
        wf_task_runner.canceled = true;
    } else {
        // Finally, notify parent about unknown messages
        notifyParent({
            error: 'unknown message'
        });
    }
});
