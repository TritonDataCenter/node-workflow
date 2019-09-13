---
title: Workflow REST API
logo-color: #aa0000
---
# Workflow REST API

This document describes the HTTP REST API for workflows and tasks, and for the
creation of Jobs and tracking their execution progress.

__This API speaks only JSON__. For every request. For all the HTTP methods.
This means that any `POST` or `PUT` request, the `Content-Type` __must be
JSON__.

(See [node-workflow](index.html) docs for information on the whole module,
not just the REST API).

# End-Points

The API is composed by the following end-points:

- `/workflows`
- `/jobs`
- `/jobs/:uuid/info`
- `/stats` (since version `0.10.0`)

`/workflows` accept any of the HTTP verbs for the usual CRUD, but `/jobs` will
not accept `PUT` since the only way to modify a Job once it has been created is
through the job's execution.

For the same reason, `/jobs/:uuid/info` will not accept neither `POST` (since the
staus information for a Job is created when the Job itself is created), nor
`DELETE` (given the status information for a job will be removed only if the job
is removed).

## GET /workflows

Retrieve a list of all the existing workflows.

### HTTP Parameters.

None.

### Status Codes

- `200 OK`: A list of existing workflows is returned. If there are no workflows,
            an empty array is returned.

### Response Body

An array of workflow objects (see `POST /workflows`).

## POST /workflows

Create a new workflow.

### HTTP Parameters

- `name`: Required. The workflow name.
- `chain[]`: Optional. The tasks to add to the workflow. Multiple values
  allowed.
- `onerror[]`: Optional. The tasks to add to the workflow fallback. Multiple
  values allowed.
- `timeout`: Optional. Timeout in seconds for workflow execution.

### Every `task` may be composed of:

- `name`: Optional. The task name.
- `body`: Required. A string enclosing a JavaScript function definition.
  The function __must__ take the parameters `job` and `cb`, where `cb` is a
  callback to be called by the function when its execution is finished. If the
  task succeeded, invoke the cb without arguments. If the task failed, invoke
  the cb with an error mssage.
- `fallback`: Optional. A string enclosing a JavaScript function definition.
  The function __must__ take the parameters `err`, `job` and `cb`, where `cb`
  is a callback to be called by the function when its execution fails;
  `err` is the error message returned by task `body`.
- `retry`: Optional. Number of times to retry the task's body before either
  failing the task, or calling the `fallback` function (when given).
- `timeout`: Optional. Timeout in seconds for task execution.

### Status Codes

- `409 Conflict`: One of the required parameters is either missing or incorrect.
  Information about the missing/incorrect parameter will be included in the
  response body.
- `201 Created`: Successful creation of the workflow. The workflow's JSON
  representation will be included in the response body, together with a
  `Location` header for the new resource. A workflow's generated `uuid` will be
  part of this `Location`, and a member of the returned workflow JSON object.

### Response Body:

    {
      uuid: UUID,
      name: 'The workflow name',
      chain: [:task, :task, ...],
      onerror: [:task, :task, ...],
      timeout: 3600
    }

#### Sample task in the response:

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
      fallback: "function(err, job, cb) {
        if (err === 'Uh, oh!, no foo.') {
          job.foo = 'bar';
          return cb(null);
        } else {
          return cb('Arise chicken, arise!');
        }
      }",
      timeout: 360
    }

## GET /workflows/:wf_uuid

### HTTP Parameters:

- `wf_uuid`: The workflow UUID.

### Status Codes:

- `404 Not Found`: There's no worlflow with the provided `wf_uuid`.
- `200 OK`: The workflow with the provided `wf_uuid` has been found and is
  returned as response body.

Note this API will not keep track of _destroyed_ workflows. When a request for
destroyed workflows is made, the HTTP Status code will be `404 Not Found`
instead of `410 Gone`.

### Response Body

Same as for `POST /workflows` + `wf_uuid`.

## PUT /workflows/:wf_uuid

### HTTP Parameters

Same as for `POST /workflows`.

### Status Codes

Same as for `POST /workflows` with the addition of:

- `404 Not Found`, when the provided `wf_uuid` cannot be found on the backend.

### Response Body

Same as for `POST /workflows`.

## DELETE /workflows/:wf_uuid

### HTTP Parameters:

- `wf_uuid`: The workflow UUID.

### Status Codes

- `204 OK`: Workflow successfully destroyed.

## GET /jobs

Retrieve a list of jobs. Without an `execution` HTTP parameter, all the existing
jobs will be retrieved. If `execution` is given, only the jobs with the given
execution status are retrieved.

### HTTP Parameters.

- `execution`: Optional. One of `succeeded`, `failed`, `running`, 'canceled'
  or `queued`.

### Status Codes

- `200 OK`: A list of existing jobs is returned, even when it's empty.

## POST /jobs

### HTTP Parameters.

- `workflow`: Required. UUID of the workflow from which the new job will be
  created.
- `exec_after`: Optional. ISO 8601 Date. Delay job execution until the provided
  time.
- `target`: The job's target, intended to restrict the creation of another job
  with this same target and parameters until this job completes.
- Any extra `k/v` pairs of parameters desired, which will be passed to the job
  object as an object like `{k1: v1, k2: v2, ...}`.

### Status Codes

- `409 Conflict`: One of the required parameters is either missing or incorrect.
  Information about the missing/incorrect parameter will be included in the
  response body.
- `201 Created`: Successful creation of the job. The job's JSON representation
  will be included in the the response body together with a `Location` header
  for the new resource. The job's generated `uuid` will be part of this
  `Location`, and a member of the returned job JSON object.

### Response Body

    {
      uuid: UUID,
      workflow_uuid: wf_uuid,
      name: 'The workflow name',
      chain: ['task object', 'task object', ...],
      onerror: ['task object', 'task object', ...],
      timeout: 3600,
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

Note this API will not keep track of _destroyed_ workflows. When a request for
destroyed workflows is made, the HTTP Status code will be `404 Not Found`
instead of `410 Gone`.

### Response Body

Same as for `POST /jobs`.

## PUT /jobs/:job_uuid

__TBD__. Response with status code `405 Method Not Allowed`.

## DELETE /jobs/:job_uuid

__TBD__. Response with status code `405 Method Not Allowed`.

## POST /jobs/:job_uuid/cancel

Cancel a job's execution. Only unfinished jobs can be canceled.

### HTTP Parameters.

- `job_uuid`: The job's UUID.

### Status Codes

- `404 Not Found`: There's no job with the provided `job_uuid`.
- `409 Conflict`: The job is already finalized and cannot be canceled.
- `200 OK`: Successfully canceled job.

### Response Body

Same than for `POST /jobs`.

## POST /jobs/:job_uuid/resume

Resumes a job's execution which was previously set to waiting by any of the Job
tasks. The job should have a *waiting* status in order to resume it.

Depending on the given parameters, the job will be either *re-queued* or flagged
as *failed*. The `chain_results` entry for the task which set the job to
`waiting` will be updated with the given `result` or `error` messages.

On success, *resume* the job will set its status from *waiting* to *queued*
again. The job will run once one of the runners has a free slot to run it.

### HTTP Parameters.

- `job_uuid`: The job's UUID.
- `result`: Optional. Result message from the remote task if it runs okay.
  Defaults to *"OK"*.
- `error`: Optional. Error message for the remote task. Note that providing any
  value for this parameter means you want to fail the job.

### Status Codes

- `404 Not Found`: There's no job with the provided `job_uuid`.
- `409 Conflict`: The job is not waiting and cannot be resumed.
- `200 OK`: Successfully resumed job.

### Response Body

Same than for `POST /jobs`.

## GET /jobs/:job_uuid/info

Detailed information about the given job. A task may cause a 3rd-party
application to execute a process which may require some time to finish. While
our task is running and waiting for the finalization of such a process, those
3rd-party applications can publish information about progress using
`POST /jobs/:job_uuid/info`; this information can then being used by other
applications interested in job results using `GET /jobs/:job_uuid/info`.

This information will consist of an arbitrary-length array, where every `POST`
request will result in a new member being appended.

### HTTP Parameters.

- `job_uuid`: The job's UUID.

### Status Codes

Same as for `GET /jobs/:job_uuid`.

### Response Body

    [
      { '10%': 'Task completed step one' },
      { '20%': 'Task completed step two' }
    ]

## POST /jobs/:job_uuid/info

### HTTP Parameters.

- `message`: Required. Object containing a message regarding the progress of
  operations.

    { '10%': 'Task completed step one' }

Note you can provide any key/value pair here.

### Status Codes

Same as for `GET /jobs/:job_uuid`.

### Response Body

None.

## PUT /jobs/:job_uuid/info

Response with status code `405 Method Not Allowed`.

## DELETE /jobs/:job_uuid/info

Response with status code `405 Method Not Allowed`.

## GET /stats

Returns a list of statistics for the current, past hour, past day and all time
system status, in terms of number of jobs on each one of the execution satatus

### HTTP Parameters.

None.

### Status Codes

- `200 OK`: A list of stats is returned, even when it's all zero counters.

### Response Body

Something with the following members but, most likely, with different numbers:

    {
      all_time: {
        queued: 0,
        failed: 0,
        succeeded: 0,
        canceled: 0,
        running: 0,
        retried: 0,
        waiting: 0 
      },
      past_24h: {
        queued: 0,
        failed: 0,
        succeeded: 0,
        canceled: 0,
        running: 0,
        retried: 0,
        waiting: 0 
      },
      past_hour: {
        queued: 0,
        failed: 3,
        succeeded: 15,
        canceled: 3,
        running: 0,
        retried: 0,
        waiting: 0 
      },
      current: {
        queued: 2,
        failed: 0,
        succeeded: 0,
        canceled: 0,
        running: 1,
        retried: 1,
        waiting: 1 
      },
    }

Please, note that `all_time` member does not contains results contained into
`past_24h`, neither this one contains results already contained into
`past_hour`, and this one ... you got it!.

<style>#forkongithub a{background:#600;color:#fff;text-decoration:none;font-family:arial, sans-serif;text-align:center;font-weight:bold;padding:5px 40px;font-size:1rem;line-height:2rem;position:relative;transition:0.5s;}#forkongithub a:hover{background:#000;color:#fff;}#forkongithub a::before,#forkongithub a::after{content:"";width:100%;display:block;position:absolute;top:1px;left:0;height:1px;background:#fff;}#forkongithub a::after{bottom:1px;top:auto;}@media screen and (min-width:800px){#forkongithub{position:absolute;display:block;top:0;right:0;width:200px;overflow:hidden;height:200px;}#forkongithub a{width:200px;position:absolute;top:60px;right:-60px;transform:rotate(45deg);-webkit-transform:rotate(45deg);box-shadow:4px 4px 10px rgba(0,0,0,0.8);}}</style><span id="forkongithub"><a href="https://github.com/kusor/node-workflow">Fork me on GitHub</a></span>
