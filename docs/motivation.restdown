---
title: Motivation
logo-color: #aa0000
---
# Overview

A workflow is effectively an orchestration. It gives you a way to decompose a
complex series of operations down to a sequence of discrete tasks within a state
machine.

The sequence of tasks is more complex than just a series. Tasks can fail, and
so you need to deal with timeouts, retries, "stuck" flows, and so forth.

One way to define a workflow and its tasks is using an arbitrarily-complex
language. Or you can keep it simple by making some assumptions:

* Code is the definition language.
* Tasks are independent. Can be used into different workflows.
* The only way to communicate between tasks is the workflow. Tasks can add,
  remove or modify properties of the workflow.
* If a task requires a specific property of the workflow to be present, the
  task can fail, or re-schedule itself within the workflow, or ...

# System design

The system must be designed with failures in mind. Tasks can fail and, as a
consequence, workflows may fail. You may want to recover from a task failure,
or from a whole workflow failure.

## Terminology

* _Task_: A single discrete operation, such as Send Email.
* _Workflow_: An abstract definition of a sequence of Tasks, including
  transition conditions and branches.
* _Job_: The execution of a workflow. It is an instance of a Workflow,
  containing all the required information to execute itself.

## System components

- A workflow and task **factory** for creating tasks, workflows and queueing
  jobs. Uses node.js.
- Alongside the **factory**, the **Workflow API** allows the creation of tasks,
  workflows and jobs through a REST API, with JSON as the payload.
- A **Status API**, used to check the status of a given job, get information
  about failures, and so forth.
- **Job runners**. These are what actually execute workflows. You can have as
  many runners as you want, and they can live anywhere on the network. Once a
  runner atomically takes a job, that job is flagged with the runner's unique
  identifier to prevent any other runner from executing it. One runner can be
  composed of multiple associated processes for executing jobs.

The factory talks to a __persistence layer__, and saves workflows and tasks.
Once a Job is created, it's saved with all the information required for its
execution, including the associated tasks' code at the moment of job creation.
This will __avoid any potential problems resulting from the modification of
task code once a Job has already been queued__.

A runner itself may unintentionally go nuts or crash while a job is being
executed, leaving the job with an inappropriated _"running"_ status. When a
runner (re)starts, there shouldn't be any job flagged with that runner's
unique identifier, nor should it have a running status. If that happens, the
first thing a runner must do upon restart is pick any job with such an invalid
state and execute the fall-back recovery branch for that job.

## Recursion and Branches

Imagine you have a task, and you specify that same task as a fall-back in case
that task fails, This isn't especially useful, since the fall-back task will
probably also fail and -- as a consequence -- the workflow will also fail ...

... wait!, or will it call the workflow's failure branch, which may also contain
_the same task!_ Obviously, this will also fail.

So, first rule: when a task is specified as part of a workflow, that same task
can neither be specified as that same task's `onerror` fall-back, nor be in the
workflow's `onerror` branch.

Now, consider the following scenario: a job is created as the result of some
action, e.g. the GET request to a given REST URI. Then -- as part
of the workflow's tasks -- another GET request is made to the same REST URI.
This will obviously result in an infinite number of jobs being created to do
exactly the same thing.

A job's target property may help us on avoid this infinite loop. While a
Workflow is something abstract, a Job can operate on a concrete target. For
example, you can use a REST URI as the target of the job, or an LDAP DN, or
whatever you need to make sure that the same job will not be queued twice.

When jobs are created and queued, they check if another job with the same target
(and the same parameters) exists. If so, the job creation will fail.

Obviously, there are some cases where you may want the same job to be queued
for the same target; for example, POST to a given URI to create a new collection
element. For that reason, a job's `parameters` are also checked with the job's
`target` when creating a new job.

If a job has failed for a given target and parameters, you may want to
create a new job after some time. This is possible since the uniqueness checks
are only made against previous jobs which are "running" or "queued", not versus
"finished" jobs (regardless of their result).

Finally, a note about multiple branches:

In theory, one may want to specify an arbitrary number of branches to be
executed, depending on workflow's tasks results. That would bring us into a
complex scenario where we make decisions like: _should we allow the same task to
be specified as part of several different branches?_.

So far I don't have a clear answer for that. In theory, only the fallbacks
should be different than their respective tasks, and the workflow's `onerror`
chain shouldn't contain any of the tasks specified as part of any of the other
branches.

Can we imagine a concrete example where multiple branches are required? If so,
we can take care of things like avoiding recursion and infinite loops, but I
think we'd rather worry about that once we find an example.

# Implementation details

1. We need a way to define tasks.
2. We need a way to define a workflow.
3. We need a way to add tasks to a workflow, and sometimes specify the exact
   position of a task in a workflow's chain.
4. We need a way to remove tasks from a workflow. Some tasks may be able to flag
   themselves as non-removable.
5. A task can fail.
6. We need to know how many times to retry a task in case of failure.
7. We need to know what to do when a task fails. Fail the workflow? Try a
   different branch? Or...?
8. A task can be stuck.
9. We need to know when to timeout a task.
10. We need to know what to do when a task times out.
11. We need to be able to instantiate a workflow in order to create a Job.
12. A Job may have parameters.
13. A Job may have a target.
14. We need to run a Job.
15. A job may have an acceptable period of time to be completed.
16. At job completion, we may want to specify another job to be completed.
17. We want to know the results of each task execution.
18. We may want to know a workflow's status at any moment.

## Task properties

- Name.
- Code to be executed.
- Timeout.
- Number of retries.
- A fall-back task to be executed if the task fails.


        {
          name: 'A Task',
          timeout: 30,
          retry: 2,
          body: function(job, cb) {
            if (!job.foo) {
              job.foo = true;
              return cb('Foo was not defined');
            }
            return cb(null);
          }
        }


Note that a task's timeout shouldn't be bigger than the workflow timeout, but
it really doesn't matter. If a task's execution exceeds the workflow timeout, it
will be failed with a 'workflow timeout' error.

## Workflow properties

- Name.
- A 'chain' of tasks to be executed.
- A global timeout.
- An alternate error branch.
- Optionally, another workflow to execute right after the current one completes


        factory.workflow({
          name: 'Sample Workflow',
          chain: [aTask, anotherTask, aTaskWhichWillFail],
          timeout: 300,
          onError: [aFallbackTask]
        }, function(err, workflow) {
          if (err) {
            throw(err);
          }
          return workflow;
        });


## Job properties

Same as a workflow, plus:

- Results for each one of the tasks.
- The job target, when given.
- The job parameters, when necessary.
- The job status (something like "queued", "running", "finished" may work).
  Note that a job is running while a task is being executed. It's possible to
  change job status to "queued" once a task has been completed, and leave the
  job there to be picked by a runner at some later moment.
- When to run the job. Maybe we want to delay execution in time?.
- Any additional properties a task may want to save with the job, to be used by
  a subsequent task.



        factory.job({
          workflow: aWorkflow,
          exec_after: '2012-01-03T12:54:05.788Z'
        }, function(err, job) {
          if (err) {
            callback(err);
          }
          aJob = job;
          callback(null, job);
        });


Some questions about other jobs options:

- Do we want to be able to "abort" a job?
- Even if it's running?
- If so, what to do when it's running and we abort? Call the `onerror`
  branch? (note that I'm beginning to think that `onerror` should be called
  `revert` sometimes, or maybe it's just an extra branch...).

See `example.js` for a detailed description on how to create Tasks, Workflows
and Jobs using NodeJS through the **factory** component.

## Workflow Runners

The system design requires that we can have workflow runners everywhere. As
many as needed, and all of them reporting health periodically. Also, it would
be desirable that runners implement a `ping` method to provide immediate
information about their status.

All runners will periodically query the backend for information about other
runners. If they detect one of those other runners has been inactive for a
configurable period of time, they will check for stale jobs associated with
that inactive runner. They will fail those jobs or run the associated `onerror`
branch.

The first thing a runner does when it boots is to register itself with the
backend (which is the same as reporting its health). At a configurable interval
a runner will try to pick queued jobs and execute them. Runners will report
activity at this same interval.

Every runner must have a unique identifier, which can either be passed in at the
runner's initialization, or be auto-generated the first time the runner is
created and saved for future runs.

Runners will spawn child processes, one process per job. The maximum number of
child processes is also configurable.

### How runners pick and execute jobs

A runner will query the backend for queued jobs (exactly the same number of
jobs as available child processes to spawn). Once the runner gets a set of
these queued jobs, it will try to obtain an exclusive lock on each job before
processing it. When a job is locked by a runner, it will not be found by other
runners searching for queued jobs.

Once the runner has an exclusive lock over the job, it'll change job status
from _queued_ to _running_, and begin executing the associated tasks.

In order to execute the job, the runner will spawn a child process, and pass
it all the information about the job; child processes don't have access to
the backend, just to the job, which must be a JSON object.

Note that everything must be executed within the acceptable amount of time
provided for the job. If this time expires, the job execution will fail and
the `onerror` branch will be executed when given.

### Task execution:

A runner will then try to execute the `job` chain of tasks, in order. For every
task, the runner will try to execute the task `body` using the node.js VM API,
effectively as a separate process. Every task will get as arguments the job and
a callback. A task should call the callback once it's completed.


    // A task body:
    function(job, cb) {
      // Task stuff here:
      cb(null);
    }


If a task succeeds, it will call the callback without `error`:

    cb(null);

Otherwise, the task should call the callback with an error message:

    cb('whatever the error reason');

These error messages will be available for the task's `onerror` function, in
order to allow a task's fallback to decide if it can recover the task from a
failure.

It's also possible to set a specific timeout for every task execution.

If a task fails, or if the task timeout is reached, the runner will check if
we've exceeded the number of retries for the task. If that's not the case,
it'll try to execute the task again.

Once the max number of retries for a task has been reached, the runner will
check if the task has an `onerror` fallback. If that's the case, it'll call it
with the error which caused the failure, as follows:

    task.onerror(error, job, cb);

The same logic as for task bodies can be applied to `onerror` fallbacks.

Note that __tasks run sandboxed__. Only the node modules we specify to the
runner at initialization time, alongside with `setTimeout`, `clearTimeout`,
`setInterval` and `clearInterval` global functions, will be available for
task `body` and `onerror` functions (this will be configurable).

All the task results will be saved in order on the job's property
`chain_results`. For every task, the results will be something like:

    {
      error: '',
      results: 'OK'
    }

or, for a task which failed:

    {
      error: 'Whatever the error reason',
      results: ''
    }

If the task fails because its `onerror` fallback failed, or because the task
doesn't have such a fallback, the job's `onerror` chain will be invoked if
present.

The logic to execute the job's `onerror` chain is exactly the same as we've
described here for the main `chain` of tasks.

Once the job is finished, either successfully or right after a failure, or even
in the case a task tells the runner to _re-queue_ the job, the child process
running the job will communicate to runner the results. The runner will save
back those results to the backend, and either finish the job, or re-queue it
for another runner.

<style>#forkongithub a{background:#600;color:#fff;text-decoration:none;font-family:arial, sans-serif;text-align:center;font-weight:bold;padding:5px 40px;font-size:1rem;line-height:2rem;position:relative;transition:0.5s;}#forkongithub a:hover{background:#000;color:#fff;}#forkongithub a::before,#forkongithub a::after{content:"";width:100%;display:block;position:absolute;top:1px;left:0;height:1px;background:#fff;}#forkongithub a::after{bottom:1px;top:auto;}@media screen and (min-width:800px){#forkongithub{position:absolute;display:block;top:0;right:0;width:200px;overflow:hidden;height:200px;}#forkongithub a{width:200px;position:absolute;top:60px;right:-60px;transform:rotate(45deg);-webkit-transform:rotate(45deg);box-shadow:4px 4px 10px rgba(0,0,0,0.8);}}</style><span id="forkongithub"><a href="https://github.com/kusor/node-workflow">Fork me on GitHub</a></span>
