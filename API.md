# Workflow & Status API

This document describes the HTTP REST API for workflows and tasks, and for the
creation of Jobs and tracking their execution progres.

__This API speaks only JSON__.

## End-Points

The API is composed by the following end-points:

- `/workflows`
- `/tasks`
- `/jobs`
- `/jobs/:uuid/status`
- `/failed_jobs`

Both, `/workflows` and `/tasks` accept any of the HTTP verbs for the usual CRUD
but `/jobs` will not accept `PUT`, given the only way to modify a Job once it
has been created is through execution.

For the same reason, `/jobs/:uuid/status` will not accept neither `POST`
requests, since the staus information for a Job is created when the Job itself
is created, neither `DELETE`, given the status information for a job will be
removed only if the job is removed.

Finally, the end-point `/failed_jobs` is merely a filter for `/jobs`, which
just return only the list of jobs whose execution failed.

## GET /workflows

Retrieve a list of all the existing workflows.

### HTTP Parameters.

None.

### Status Codes

- `204 No Content`: No workflows created yet.
- `200 OK`: A list of existing workflows is returned.

### Response Body

An array of workflow objects, (see `POST /workflows`).

## POST /workflows

Create a new workflow.

### HTTP Parameters

- `name`: The workflow name. Required.
- `chain[]`: UUIDs for tasks to add to the workflow. Optional. Multiple values
  allowed.
- `onerror[]`: UUIDs for tasks to add to the workflow fallback. Optional.
  Multiple values allowed.
- `timeout`: Optional timeout, in minutes, for the workflow execution.

### Status Codes

- `409 Conflict`: One of the required parameters is either missing or incorrect
  Information about the missing/incorrect parameter will be included into
  response body.
- `201 Created`: Successful creation of the workflow. The workflow's JSON
  representation will be included into the response body, together with a
  `Location` header for the new resource. Generated workflow's `uuid` will be
  part of this `Location` and a member of the returned workflow JSON object.

#### Response Body:

    {
      uuid: UUID,
      name: 'The workflow name',
      chain: [:task_uuid, :task_uuid, ...],
      onerror: [:task_uuid, :task_uuid, ...],
      timeout: 60m
    }

## GET /workflows/:wf_uuid

### HTTP Parameters:

- `wf_uuid`: The workflow UUID.

### Status Codes:

- `404 Not Found`: There's no worlflow with the provided `wf_uuid`.
- `200 OK`: The workflow with the provided `wf_uuid` has been found and is
  returned as response body.

Note this API will not keep track of _destroyed_ workflows, therefore, when a
request for such workflows is made, the HTTP Status code will be `404 Not Found`
instead of `410 Gone`.

### Response body

Same than for `POST /workflows` + `wf_uuid`.

## PUT /workflows/:wf_uuid

### HTTP Parameters

Same than for `POST /workflows`.

### Status Codes

Same than for `POST /workflows` with the addition of:

- `404 Not Found`, when the provided `wf_uuid` cannot be found on the backend.

### Response body

Same than for `POST /workflows`.

## DELETE /workflows/:wf_uuid

### HTTP Parameters:

- `wf_uuid`: The workflow UUID.

### Status Codes

- `200 OK`: Workflow successfully destroyed.

## GET /tasks

Retrieve a list of existing tasks.

### HTTP Parameters.

None.

### Status Codes

- `204 No Content`: No tasks created yet.
- `200 OK`: A list of existing tasks is returned.

### Response Body

An array of task objects, (see `POST /tasks`).

## POST /tasks

Create a new task.

### HTTP Parameters

- `name`: The task name. Required.
- `body`: Required. A string enclosing a JavaScript function definition.
  The function __must__ take the parameters `job` and `cb`, where `cb` is a
  callback to be called by the function when its execution is finished, either
  without any arguments, (_task succeed_), or with an error message, (_task
  failed_).
- `onerror`: Optional. A string enclosing a JavaScript function definition.
  The function __must__ take the parameters `err`, `job` and `cb`, where `cb`
  is a callback to be called by the function when its execution is finished,
  either without any arguments, (_task succeed_), or with an error message, 
  (_task failed_). `err` is the error message returned by task `body`.
- `retry`: Optional. Number of times to retry the task's body execution
  before either fail the task or call the `onerror` function, when given.
- `timeout`: Optional timeout, in minutes, for the task execution.

### Status Codes

- `409 Conflict`: One of the required parameters is either missing or incorrect
  Information about the missing/incorrect parameter will be included into
  response body.
- `201 Created`: Successful creation of the task. The task's JSON
  representation will be included into the response body, together with a
  `Location` header for the new resource. Generated task's `uuid` will be
  part of this `Location` and a member of the returned task JSON object.

#### Response Body:

    {
      uuid: UUID,
      name: 'The task name',
      body: "function(job, cb) {
        if (job.foo) {
          return cb(null);
        } else {
          return cb('Uh, oh!, no foo.');
        }
      }",
      onerror: "function(err, job, cb) {
        if (err === 'Uh, oh!, no foo.') {
          job.foo = 'bar';
          return cb(null);
        } else {
          return cb('Arise chicken, arise!');
        }
      }",
      timeout: 6m
    }


## GET /tasks/:task_uuid

### HTTP Parameters:

- `task_uuid`: The task UUID.

### Status Codes:

- `404 Not Found`: There's no task with the provided `task_uuid`.
- `200 OK`: The task with the provided `task_uuid` has been found and is
  returned as response body.

Note this API will not keep track of _destroyed_ tasks, therefore, when a
request for such tasks is made, the HTTP Status code will be `404 Not Found`
instead of `410 Gone`.

### Response body

Same than for `POST /task` + `task_uuid`.

## PUT /tasks/:task_uuid

### HTTP Parameters

Same than for `POST /tasks`.

### Status Codes

Same than for `POST /tasks` with the addition of:

- `404 Not Found`, when the provided `task_uuid` cannot be found on the backend.

### Response body

Same than for `POST /tasks`.


## DELETE /tasks/:task_uuid

### HTTP Parameters:

- `task_uuid`: The task UUID.

### Status Codes

- `200 OK`: Task successfully destroyed.

## GET /jobs

Retrieve a list of all the existing jobs.

### HTTP Parameters.

None.

### Status Codes

- `204 No Content`: No jobs created yet.
- `200 OK`: A list of existing jobs is returned.

## POST /jobs

### HTTP Parameters.

- `workflow_uuid`: Required. UUID of the workflow from which the new job will
  be created.
- `exec_after`: Optional, ISO 8601 Date. Delay job execution until the provided
  time.
- `target`: The job's target, intended to restrict the creation of another job
  with this same target and same extra parameters while this one execution hasn't
  finished.
- Any extra `k/v` pairs of parameters desired, which will be passed to the job
  object as an object like `{k1: v1, k2: v2, ...}`.

### Status Codes

- `409 Conflict`: One of the required parameters is either missing or incorrect
  Information about the missing/incorrect parameter will be included into
  response body.
- `201 Created`: Successful creation of the job. The job's JSON
  representation will be included into the response body, together with a
  `Location` header for the new resource. Generated job's `uuid` will be
  part of this `Location` and a member of the returned job JSON object.

### Response Body

    {
      uuid: UUID,
      worflow_uuid: wf_uuid,
      name: 'The workflow name',
      chain: ['task object', 'task object', ...],
      onerror: ['task object', 'task object', ...],
      timeout: 60m,
      exec_after: new Date().toISOString(),
      target: '/some/uri',
      params: {
        k1: v1,
        k2: v2
      },
      chain_results: [{result: 'OK', error: ''}, ...],
      onerror_results: [{result: 'OK', error: ''}, ...],
      execution: 'queued' ('running'|'failure'|'success')
    }


## GET /jobs/:job_uuid

### HTTP Parameters.

- `job_uuid`: The job's UUID.

### Status Codes

- `404 Not Found`: There's no job with the provided `job_uuid`.
- `200 OK`: The task with the provided `job_uuid` has been found and is
  returned as response body.

Note this API will not keep track of _destroyed_ jobs, therefore, when a
request for such tasks is made, the HTTP Status code will be `404 Not Found`
instead of `410 Gone`.

### Response Body

Same than for `POST /jobs`.

## PUT /jobs/:job_uuid

Response with status code `405 Method Not Allowed`.

## DELETE /jobs/:job_uuid

__TBD__. Response with status code `405 Method Not Allowed`.

## GET /jobs/:job_uuid/status

Detailed status information for the given job. A task may result into a 3rd
party application executing a process which may require some time/steps to
finish. While our task is running and waiting for the finalization of such
process, those 3rd party applications can publish information about progress
using `POST|PUT /jobs/:job_uuid/status`; this information could then being used
by other applications interested on job results using
`GET /jobs/:job_uuid/status`.

This `status` information will consist into an arbitrary length array, where
every `POST|PUT` request will result in a new member being appended.

In order to save HTTP requests to API consumers, the whole `job` + the `status`
information will be retrieved on `GET` requests to this URI.

### HTTP Parameters.

- `job_uuid`: The job's UUID.

### Status Codes

Same than for `GET /jobs/:job_uuid`.

### Response Body

Same than for `POST /jobs`, plus the detailed information provided by 3rd party
applications executing task's requests.

## POST | PUT /jobs/:job_uuid/status

### HTTP Parameters.

- `message`: Required. String containing a message regarding operations progresses.

### Status Codes

Same than for `GET /jobs/:job_uuid`.

### Response Body

None.

## DELETE /jobs/:job_uuid/status

Response with status code `405 Method Not Allowed`.

## GET /failed_jobs

__TBD__. Maybe just `GET /jobs?execution=failed`?.
Retrieve a list of all the failed jobs.

### HTTP Parameters.

None.

### Status Codes

- `204 No Content`: No jobs created yet.
- `200 OK`: A list of existing failed jobs is returned.

