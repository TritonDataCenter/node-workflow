# Overview

So a workflow is effectively an orchestration.

You want a workflow, because it gives you a way to decompose a complex series
of operations down to a sequence of discreet tasks with a state machine.

The sequence of tasks is more complex than just a series. The tasks of course
can fail, and so you need to deal with timeouts, retries, "stuck" flows, etc.

You can define a workflow and its tasks using an arbitrarily complex language.
Or you can keep it simple by taking some assumptions:

* Code is the definition language.
* Tasks are independent. Can be used into different workflows.
* The only way of communication between tasks is the workflow. Tasks can add,
  remove or modify properties of the workflow.
* If a task requires a specific property of the workflow to be present, the
  task can fail, or re-schedule itself within the workflow, or ...

# System design

System needs to be designed with failures in mind. Tasks can fail and, as a
consequence, workflows may fail. You may want to recover from a task failure,
or from a whole workflow failure.

The system itself may also fail due to any unexpected reason.

## Terminology

* _Task_: A single discreet operation, such as Send Email.
* _Workflow_: An abstract definition of a sequence of Tasks, including
  transition conditions, and branches.
* _Job_: An instance of a Workflow containing all the required information
  to execute itself and, eventually, a target.

## System components

- A workflow and task **factory**, where you can create tasks, workflows and queue
  jobs.
- An **Status API**, where you can check the status of a given job, get information
  about failures, ...
- **Job runners**. You can have as many runners as you want. Runners can live
  everywhere. Once a runner "picks" a job, that job is flagged with the runner
  identifier so no other runner attempts to execute it. One runner can be
  composed of multiple associated processes to execute jobs.

The factory talks to a __persistence layer__, and saves workflows and tasks. Also,
once a Job is created, it's saved with all the required information, including
the tasks code at the moment of job creation. This will __avoid any potential
problems resulting of underlaying modifications of tasks once a Job has been
queued__.

A runner itself may go dive nuts while a job is being executed, leaving the
job into an unappropriated _"running"_ status. When a runner starts, there
shouldn't be any job flagged with the runner identifier and, furthermore,
it shouldn't be on a running status. If that happens, the first thing a runner
must do on restart is to pick any job into such invalid state and execute
the fall-back recovery branch for such job.

## Recursion and Branches

Imagine you have a task and, as the fall-back for the case that task fails,
you specify that same task. Not very useful; not too bad since the fall-back
task will also fail and, as a consequence, the workflow will fail ...

... wait!, or will call the workflow's failure branch which may also contain
_the same task!_ which, obviously, will fail again.

So, first rule: When a task is specified as part of a workflow, that same task
can not be specified as the task itself `onerror` fall-back, neither to the
workflow's `onerror` branch.

Now, let's think about the following scenario: a job is created as the result
of some action, for example, a GET request to a given REST URI. Then, as part
of the workflow's tasks, another GET request is made to the same REST URI,
which will obviously result into infinite number of jobs being created to do
exactly the same thing.

Job's target property may help us on avoiding this infinite loop problem. While
a Workflow is something "abstract", a Job may "operate" over a concrete target.
For example, you can use a REST URI as the target of the job, or an LDAP DN, or
whatever you need to make sure that the same job will not be queued twice.

When jobs are created and queued, they will check if another job with the same
target (and the same parameters) exists and, if that's the case, the job creation
will fail.

Obviously, there are some cases where you may want the same job to be queued
exactly for the same target; for example, POST to a given URI to create a new
collection element. That's the reason job's `parameters` are also checked with
job's `target` in order to allow or not creation and queueing of a new job.

In the case a job has failed for a given target and parameters, you may want to
create a new one after some time. This is perfectly possible since the previous
job uniqueness checks are made only versus "running" or "queued" jobs, not versus
"finished" jobs, whatever is their result.

Finally, a note about multiple branches:

In theory, one may want to specify any arbitrary number of branches to be
executed depending on workflow's tasks results. That would bring us into a complex
scenario where we should take decisions like: _should we allow the same task to
be specified as part of several different branches?_.

So far I haven't got a clear answer for that. In theory, only the fall-backs
should be different than their respective tasks, and the workflow's `onerror`
chain shouldn't contain any of the tasks specified as part of any of the other
branches.

Anyhow, can we imagine a concrete scenario where multiple branches are required?.
I mean, with an example. If so, we can take care of things like avoiding
recursion and infinite loops being caused there, but I'm thinking we'd rather
care of that once we find the example.

# Implementation details

1. You need a way to define tasks.
2. Need a way to define a workflow.
3. Need a way to add tasks to a workflow. Sometimes specify the exact position
   of a task on the workflow chain.
4. Need a way to remove tasks from the workflow. Some tasks may be able to flag
   themselves as no-removable.
5. A task can fail.
6. You need to know how many times you want to retry a task in case of failure.
7. You need to know what to do when a task fail. Fail the workflow?, try a
   different branch?, ...
8. A task can be stuck.
9. You need to know when to timeout a task.
10. You need to know what to do when a given task timeout is over.
11. You need to be able to instantiate a workflow in order to create a Job.
12. A Job may have parameters.
13. A Job may have a target.
14. You need to run a Job.
15. A job may have an acceptable period of time to be completed.
16. At job end, you may want to be able to specify another job to be completed.
17. Of course, you want to know the results of each task execution.
18. You may want to know the workflow status at any moment.

## Task properties:

- A name,
- code to be executed,
- timeout,
- number of retries, when proceed
- a fall-back task to be executed when the task fails.

Note that a task's timeout shouldn't be bigger than the workflow timeout, but
it really doesn't matter. If a task execution exceeds the workflow timeout it
will be failed with 'workflow timeout' error.

## Workflow properties:

- A name.
- The 'chain' of tasks to be executed.
- A global timeout.
- Alternate error branch.
- Optionally, another workflow to execute right after the current one is completed.

## Job properties:

Same than the workflow plus:

- Results for each one of the tasks (we need to decide about tasks results format)
- The job target, when given.
- The job parameters, when necessary.
- The job status (something like "queued", "running", "finished" may work).
  Note that a job is running while a task is being executed. It's perfectly
  possible to change job status to "queued" once a task has been completed, and
  leave the job there to be picked by a runner at some later moment.
- When to run the job. May we want to delay execution in time?.
- Any additional property a task may want to save with the job to be used by
  a subsequent task.

Some questions about other jobs options:

- Do we want to be able to "abort" a job?.
- Even if it's running?
- And, if so, what to do when it's running and we abort?, call `onerror`
  branch?, (note I'm beginning to think that `onerror` should be called
  `revert` sometimes, or maybe it's just an extra branch ...).


        workflow.create('the Workflow Name', {
            chain: [aTask, anotherTask],
            timeout: 5m,
            onError: 'some Other Branch',
            next: 'another workflow'
        });
        // Instantiates a new Workflow object, sets properties and
        // stores it on the backend

        workflow.task('aTask', {
            timeout: 30s,
            retry: 3,
            onError: 'some other task'
        }, function(job) {
            // ... do stuff here
            return next();
        });
        // Instantiates a new Task object, sets properties and
        // stores it on the backend

        // A position? or allow specify "right before/after task with name 'Foo'?"
        workflow.addTask('the Workflow Name', 'a New Task' [, position?]);
        workflow.removeTask('the Workflow Name', 'a Task');

        // Instantiates a new Job object and stores it on the backend
        job.create('the Workflow Name' [, target [, {parameters}]]);
        // Note that, while a workflow may have references to the backend
        // for tasks, the job will contain a complete definition of each task
        // instead of just a pointer to them
        runner.run([job]); // Runners should pick pending jobs by themselves


