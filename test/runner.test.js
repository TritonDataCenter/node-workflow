// Copyright 2012 Pedro P. Candel <kusorbox@gmail.com>. All rights reserved.
// Copyright (c) 2018, Joyent, Inc.

var util = require('util');
var path = require('path');
var fs = require('fs');
var test = require('tap').test;
var uuid = require('uuid');
var vasync = require('vasync');
var WorkflowRunner = require('../lib/runner');
var Factory = require('../lib/index').Factory;
var exists = fs.exists || path.exists;
var createDTrace = require('../lib/index').CreateDTrace;

var backend, identifier, runner, factory;

var config = {};

var okTask = {
    name: 'OK Task',
    retry: 1,
    body: function (_job, cb) {
        cb(null);
    }
};
var failTask = {
    retry: 1,
    name: 'Fail Task',
    body: function (job, cb) {
        job.log.info('recording some info');
        cb('Fail task error');
    }
};
var failTaskWithError = {
    retry: 1,
    name: 'Fail Task with error',
    body: function (job, cb) {
        job.log.info('recording some info');
        cb(new Error('Fail task error'));
    }

};
var failTaskWithJobRetry = {
    retry: 1,
    name: 'Fail Task with job retry',
    body: function (_job, cb) {
        cb('retry');
    }
};
var failTaskWithJobWait = {
    retry: 1,
    name: 'Fail Task with job wait',
    body: function (_job, cb) {
        cb('wait');
    }
};
var taskWithModules = {
    name: 'OK Task with modules',
    retry: 1,
    body: function (_job, cb) {
        if (typeof (uuid) !== 'function') {
            cb('uuid module is not defined');
            return;
        }
        cb(null);
    },
    modules: {
        uuid: 'uuid'
    }
};
var okWf, failWf, theJob, failWfWithError, failWfWithRetry;

var helper = require('./helper');

var DTRACE = createDTrace('workflow');

test('throws on missing opts', function (t) {
    t.throws(function () {
        WorkflowRunner();
    }, 'The "opts" argument must be of type object');
    t.end();
});


test('throws on missing backend', function (t) {
    t.throws(function () {
        WorkflowRunner(config);
    }, 'The "opts.backend" argument must be of type object');
    t.end();
});


test('throws on missing dtrace', function (t) {
    config = helper.config();
    t.throws(function () {
        WorkflowRunner(config);
    }, 'The "opts.dtrace" argument must be of type object');
    t.end();
});


test('setup', function (t) {
    config.dtrace = DTRACE;
    identifier = config.runner.identifier;
    config.logger = {
        streams: [ {
            level: 'info',
            stream: process.stdout
        }, {
            level: 'trace',
            path: path.resolve(__dirname, './test.runner.log')
        }]
    };
    runner = WorkflowRunner(config);
    t.ok(runner);
    t.ok(runner.backend, 'backend ok');
    backend = runner.backend;
    runner.init(function (err0) {
        t.ifError(err0, 'runner init error');
        factory = Factory(backend);
        t.ok(factory);

        // okWf:
        factory.workflow({
            name: 'OK wf',
            chain: [okTask, failTaskWithJobWait, taskWithModules],
            timeout: 60
        }, function (err1, wf1) {
            t.ifError(err1, 'ok wf error');
            t.ok(wf1, 'OK wf OK');
            okWf = wf1;
            // failWf:
            factory.workflow({
                name: 'Fail wf',
                chain: [failTask],
                timeout: 60
            }, function (err2, wf2) {
                t.ifError(err2, 'Fail wf error');
                t.ok(wf2, 'Fail wf OK');
                failWf = wf2;
                factory.workflow({
                    name: 'Fail wf with error',
                    chain: [failTaskWithError],
                    timeout: 60
                }, function (err3, wf3) {
                    t.ifError(err3, 'Fail wf with error');
                    t.ok(wf3, 'Fail wf with error OK');
                    failWfWithError = wf3;
                    factory.workflow({
                        name: 'Fail wf with retry',
                        chain: [failTaskWithJobRetry],
                        timeout: 60,
                        max_attempts: 3
                    }, function (err4, wf4) {
                        t.ifError(err4, 'Fail wf with retry');
                        t.ok(wf4, 'Fail wf with retry OK');
                        failWfWithRetry = wf4;
                        backend.getRunners(function (err5, runners) {
                            t.ifError(err5, 'get runners error');
                            t.ok(runners[identifier], 'runner id ok');
                            t.ok(new Date(runners[identifier]),
                                'runner timestamp ok');
                            t.end();
                        });
                    });
                });
            });
        });
    });
});


test('runner identifier', function (t) {
    const cfg = {
        backend: helper.config().backend,
        dtrace: DTRACE
    };
    const aRunner = WorkflowRunner(cfg);
    var ident;
    // run getIdentifier twice, one to create the file,
    // another to just read it:
    aRunner.init(function (err) {
        t.ifError(err, 'runner init error');
        aRunner.getIdentifier(function (err1, id) {
            t.ifError(err1, 'get identifier error');
            t.ok(id, 'get identifier id');
            ident = id;
            aRunner.getIdentifier(function (err2, id2) {
                t.ifError(err2, 'get identifier error');
                t.equal(id2, ident, 'correct id');
                aRunner.backend.quit(function () {
                    t.end();
                });
            });
        });
    });
});


test('runner run job now', function (t) {
    var d = new Date();
    t.ok(runner.runNow({exec_after: d.toISOString()}));
    // Set to the future, so it shouldn't run:
    d.setTime(d.getTime() + 10000);
    t.ok(!runner.runNow({exec_after: d.toISOString()}));
    t.end();
});


test('runner next run', function (t) {
    var d = Date.now();
    var job = {
        started: d,
        elapsed: 1.23,
        max_delay: 7000,
        initial_delay: 1000,
        num_attempts: 0
    };
    t.equal(new Date(runner.nextRun(job)).getTime(), d + 1230 + 1000);
    job.num_attempts++;
    t.equal(new Date(runner.nextRun(job)).getTime(), d + 1230 + 2000);
    job.num_attempts++;
    t.equal(new Date(runner.nextRun(job)).getTime(), d + 1230 + 4000);
    job.num_attempts++;
    t.equal(new Date(runner.nextRun(job)).getTime(), d + 1230 + 7000);
    job.num_attempts++;
    t.equal(new Date(runner.nextRun(job)).getTime(), d + 1230 + 7000);
    t.end();
});


test('idle runner', function (t) {
    runner.run();
    backend.idleRunner(runner.identifier, function (err) {
        t.ifError(err, 'idle runner error');
        factory.job({
            workflow: okWf.uuid,
            exec_after: '2012-01-03T12:54:05.788Z'
        }, function (err1, job1) {
            t.ifError(err1, 'job error');
            t.ok(job1, 'run job ok');
            theJob = job1;
            // The job is queued. The runner is idle. Job should remain queued:
            backend.getJob(theJob.uuid, function (err2, job2) {
                t.ifError(err2, 'run job get job error');
                t.equal(job2.execution, 'queued', 'job execution');
                runner.quit(function () {
                    t.end();
                });
            });
        });
    });
});


test('run job', function (t) {
    // Let's remove the idleness of the runner so it will pick the job
    backend.wakeUpRunner(runner.identifier, function (err) {
        t.ifError(err, 'wake up runner error');
        runner.run();
        setTimeout(function () {
            runner.quit(function () {
                backend.getJob(theJob.uuid, function (err1, job1) {
                    t.ifError(err1, 'run job get job error');
                    t.equal(job1.execution, 'waiting', 'job execution');
                    t.equal(job1.chain_results[0].result, 'OK');
                    t.equal(job1.chain_results[1].result, 'OK');
                    theJob = job1;
                    t.end();
                });
            });
        }, 1000);
    });
});


test('re-run waiting job', function (t) {
    backend.resumeJob(theJob, function (err, job) {
        t.ifError(err, 'resume job error');
        theJob = job;
        runner.run();
        setTimeout(function () {
            runner.quit(function () {
                backend.getJob(theJob.uuid, function (err1, job1) {
                    t.ifError(err1, 'run job get job error');
                    t.equal(job1.execution, 'succeeded', 'job execution');
                    t.equal(job1.chain_results[0].result, 'OK');
                    t.equal(job1.chain_results[1].result, 'OK');
                    t.end();
                });
            });
        }, 1000);
    });
});


test('run job which fails', function (t) {
    var aJob;
    factory.job({
        workflow: failWf.uuid,
        exec_after: '2012-01-03T12:54:05.788Z'
    }, function (err, job) {
        t.ifError(err, 'job error');
        t.ok(job, 'job ok');
        aJob = job;
        runner.run();
        setTimeout(function () {
            runner.quit(function () {
                backend.getJob(aJob.uuid, function (err1, job1) {
                    t.ifError(err1, 'get job error');
                    t.equal(job1.execution, 'failed', 'job execution');
                    t.equal(job1.chain_results[0].error, 'Fail task error');
                    t.end();
                });
            });
        }, 300);
    });
});


test('run job which fails with error instance', function (t) {
    var aJob;
    factory.job({
        workflow: failWfWithError.uuid,
        exec_after: '2012-01-03T12:54:05.788Z'
    }, function (err, job) {
        t.ifError(err, 'job error');
        t.ok(job, 'job ok');
        aJob = job;
        runner.run();
        setTimeout(function () {
            runner.quit(function () {
                backend.getJob(aJob.uuid, function (err1, job1) {
                    t.ifError(err1, 'get job error');
                    t.equal(job1.execution, 'failed', 'job execution');
                    t.ok(job1.chain_results[0].error.name);
                    t.ok(job1.chain_results[0].error.message);
                    t.equal(job1.chain_results[0].error.message,
                        'Fail task error');
                    t.end();
                });
            });
        }, 300);
    });
});


test('a job that is retried', function (t) {
    var aJob;
    factory.job({
        workflow: failWfWithRetry.uuid,
        exec_after: '2012-01-03T12:54:05.788Z'
    }, function (err, job) {
        t.ifError(err, 'job error');
        t.ok(job, 'job ok');
        aJob = job;
        runner.run();
        setTimeout(function () {
            runner.quit(function () {
                backend.getJob(aJob.uuid, function (err1, job1) {
                    t.ifError(err1, 'get job error');
                    t.equal(job1.execution, 'retried', 'job execution');
                    t.equal(job1.chain_results[0].error, 'retry', 'error');
                    var prevJob = job1;
                    backend.getJob(job1.next_attempt, function (err2, job2) {
                        t.ifError(err2, 'get job error');
                        t.equal(job2.execution, 'retried', 'job execution');
                        t.equal(job2.chain_results[0].error, 'retry', 'error');
                        t.equal(prevJob.uuid, job2.prev_attempt);
                        var midJob = job2;
                        backend.getJob(job2.next_attempt,
                            function getJCb(err3, job3) {
                            t.ifError(err3, 'get job error');
                            t.equal(job3.execution, 'retried', 'job execution');
                            t.equal(job3.chain_results[0].error, 'retry');
                            t.equal(midJob.uuid, job3.prev_attempt);
                            t.notOk(job3.next_attempt);
                            t.end();
                        });
                    });
                });
            });
        }, 2000);
    });
});


test('inactive runners', function (t) {
    // Add another runner, which we'll set as inactive
    var theUUID = uuid(),
    cfg = {
        backend: helper.config().backend,
        runner: {
            identifier: theUUID,
            forks: 2,
            run_interval: 250
        },
        dtrace: DTRACE
    },
    anotherRunner = WorkflowRunner(cfg);
    t.ok(anotherRunner, 'another runner ok');
    // Init the new runner, then update it to make inactive
    anotherRunner.init(function (err) {
        t.ifError(err, 'another runner init error');
        // Now we quit the new runner, and outdate it:
        anotherRunner.quit(function () {
            runner.inactiveRunners(function (err1, runners1) {
                t.ifError(err1, 'inactive runners error');
                t.ok(util.isArray(runners1), 'runners is array');
                t.equal(runners1.length, 0, 'runners length');
                backend.runnerActive(
                    anotherRunner.identifier,
                    '2012-01-03T12:54:05.788Z',
                    function (err2) {
                        t.ifError(err2, 'set runner timestamp error');
                        runner.inactiveRunners(function (err3, runners3) {
                            t.ifError(err3, 'inactive runners error');
                            t.ok(util.isArray(runners3), 'runners is array');
                            t.equal(runners3.length, 1, 'runners length');
                            t.equal(runners3[0], theUUID, 'runner uuid error');
                            anotherRunner.backend.quit(function () {
                                t.end();
                            });
                        });
                  });
            });
        });
    });
});


test('stale jobs', function (t) {
    // Add another runner, which we'll set as inactive
    var cfg = {
        backend: helper.config().backend,
        runner: {
            identifier: uuid(),
            forks: 2,
            run_interval: 250
        },
        dtrace: DTRACE
    };

    vasync.pipeline({
        arg: {
            anotherRunner: WorkflowRunner(cfg)
        },
        funcs: [
            // Create a job and store as `ctx.aJob`
            function createJob(ctx, next) {
                factory.job({
                    workflow: okWf.uuid,
                    exec_after: '2012-01-03T12:54:05.788Z'
                }, function (err, job) {
                    t.ifError(err, 'job error');
                    t.ok(job, 'run job ok');
                    ctx.aJob = job;
                    backend.getJob(ctx.aJob.uuid, function (err1, job1) {
                        t.ifError(err1, 'get job err');
                        t.equal('queued', job1.execution, 'Job is queued');
                        ctx.aJob = job1;
                        next();
                    });
                });
            },
            function runJob(ctx, next) {
                backend.runJob(
                    ctx.aJob.uuid,
                    ctx.anotherRunner.identifier,
                    function runJobCb(err, job) {
                        t.ifError(err, 'backend run job error');
                        t.equal('running', job.execution, 'Job is running');
                        ctx.aJob = job;
                        ctx.anotherRunner.quit(next);
                });
            },
            function checkStaleJobs(_, next) {
                runner.staleJobs(function (err, jobs) {
                    t.ifError(err, 'stale jobs error');
                    t.equivalent(jobs, [], 'stale jobs empty');
                    next();
                });
            },
            function outdateRunner(ctx, next) {
                // The runner will be inactive so, any job flagged
                // as owned by this runner will be stale
                backend.runnerActive(
                    ctx.anotherRunner.identifier,
                      '2012-01-03T12:54:05.788Z',
                      function (err) {
                          t.ifError(err, 'set runner timestamp error');
                          next();
                      });
            },
            function reCheckStaleJobs(ctx, next) {
                runner.staleJobs(function (err, jobs) {
                    t.ifError(err, 'stale jobs error');
                    t.equivalent(jobs, [ctx.aJob.uuid], 'stale jobs not empty');
                    next();
                });
            },
            function finishJob(ctx, next) {
                backend.finishJob(ctx.aJob, function finishJobCb(err, job) {
                    t.ifError(err, 'finish job err');
                    t.ok(job, 'finish job ok');
                    next();
                });
            },
            function triCheckStaleJobs(_, next) {
                runner.staleJobs(function (err, jobs) {
                    t.ifError(err, 'stale jobs error');
                    t.equivalent(jobs, [],
                        'Only not finished jobs can be stale');
                    next();
                });
            }
        ]
    }, function pipeCb(pipeErr) {
        t.end(pipeErr);
    });

});


test('timeout job', function (t) {
    vasync.pipeline({
        arg: {},
        funcs: [
            function createWorkflow(ctx, next) {
                factory.workflow({
                    name: 'Timeout wf',
                    chain: [
                        {
                            name: 'Timeout Task',
                            retry: 0,
                            body: function (_job, cb) {
                                setTimeout(function () {
                                    cb(null);
                                }, 10000);
                            }
                        }
                    ],
                    timeout: 1,
                    max_attempts: 1
                }, function (err, wf) {
                    t.ifError(err, 'Timeout wf error');
                    t.ok(wf, 'Timeout wf OK');
                    ctx.wf = wf;
                    next();
                });
            },
            function createJob(ctx, next) {
                factory.job({
                    workflow: ctx.wf.uuid,
                    exec_after: '2012-01-03T12:54:05.788Z'
                }, function (err, job) {
                    t.ifError(err, 'job error');
                    t.ok(job, 'job ok');
                    ctx.aJob = job;
                    backend.getJob(ctx.aJob.uuid, function (err1, job1) {
                        t.ifError(err1, 'get job err');
                        t.equal('queued', job1.execution, 'Job is queued');
                        ctx.aJob = job1;
                        next();
                    });
                });
            },
            function runJob(_, next) {
                backend.wakeUpRunner(runner.identifier, function (err) {
                    t.ifError(err, 'wake up runner error');
                    runner.run();
                    next();
                });
            },
            function checkJob(ctx, next) {
                // Give it room enough to timeout the job
                setTimeout(function () {
                    backend.getJob(ctx.aJob.uuid, function (err, job) {
                        t.ifError(err, 'get job err');
                        t.equal('failed', job.execution, 'Job is failed');
                        t.ok(job.chain_results, 'chain_results');
                        t.ok(job.chain_results[0].error, 'job error');
                        t.ok(job.chain_results[0].finished_at,
                            'job finished_at');
                        next();
                    });
                }, 2000);

            },
            function quitRunner(_, next) {
                runner.quit(next);
            }
        ]
    }, function pipeCb(pipeErr) {
        t.end(pipeErr);
    });
});


test('teardown', function (t) {
    var cfg_file = path.resolve(__dirname, '../workflow-indentifier');
    runner.backend.quit(function () {
        exists(cfg_file, function (exist) {
            if (exist) {
                fs.unlink(cfg_file, function (err) {
                    t.ifError(err);
                    t.end();
                });
            } else {
                t.end();
            }
        });
    });
});
