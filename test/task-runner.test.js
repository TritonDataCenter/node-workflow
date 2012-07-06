// Copyright 2012 Pedro P. Candel <kusorbox@gmail.com>. All rights reserved.
var util = require('util'),
    test = require('tap').test,
    uuid = require('node-uuid'),
    WorkflowTaskRunner = require('../lib/task-runner');

var job = {
    timeout: 180,
    workflow_uuid: 'bdfa0821-5071-4682-b965-88293149a8d2',
    name: 'A workflow name',
    exec_after: '2012-01-03T12:54:05.788Z',
    params: {
        'a': '1',
        'b': '2'
    },
    uuid: 'fb4c202d-19ed-4ed9-afda-8255aa7f38ad',
    target: '/foo/bar',
    execution: 'running',
    chain_results: [],
    chain: [],
    onerror: []
};

var task = {
    'uuid': uuid(),
    'name': 'A name',
    'body': 'Fake body'
};


var sandbox = {
    'modules': {
        'http': 'http',
        'uuid': 'node-uuid'
    },
    'foo': 'bar',
    'bool': true,
    'aNumber': 5
};

test('throws on missing opts', function (t) {
    t.throws(function () {
        return new WorkflowTaskRunner();
    }, new TypeError('opts (Object) required'));
    t.end();
});


test('throws on missing opts.job', function (t) {
    t.throws(function () {
        return new WorkflowTaskRunner({});
    }, new TypeError('opts.job (Object) required'));
    t.end();
});


test('throws on missing opts.task', function (t) {
    t.throws(function () {
        return new WorkflowTaskRunner({job: job});
    }, new TypeError('opts.task (Object) required'));
    t.end();
});


test('throws on incorrect opts.sandbox', function (t) {
    t.throws(function () {
        return new WorkflowTaskRunner({
            job: job,
            task: task,
            sandbox: 'foo'
        });
    }, new TypeError('opts.sandbox must be an Object'));
    t.end();
});


test('throws on opts.task.body not a function', function (t) {
    t.throws(function () {
        return new WorkflowTaskRunner({
            job: job,
            task: task
        });
    }, new TypeError('opt.task.body (String) must be a Function source'));
    task.body = '5 === 5';
    t.throws(function () {
        return new WorkflowTaskRunner({
            job: job,
            task: task
        });
    }, new TypeError('opt.task.body (String) must be a Function source'));
    t.end();
});


test('a task which succeeds on 1st retry', function (t) {
    task.body = function (job, cb) {
        return cb(null);
    }.toString();

    job.chain.push(task);

    var wf_task_runner = new WorkflowTaskRunner({
        job: job,
        task: task
    });

    t.ok(wf_task_runner.uuid);
    t.equal(typeof (wf_task_runner.body), 'function');

    wf_task_runner.runTask(function (msg) {
        t.ok(msg.result);
        t.ifError(msg.error, 'task error');
        t.ok(msg.job);
        t.equal(msg.cmd, 'run');
        t.equal(msg.task_name, task.name);
        t.end();
    });

});


test('sandbox modules and variables', function (t) {
    // Or javascriptlint will complain regarding undefined variables:
    task.body = 'function (job, cb) {\n' +
        'if (typeof (uuid) !== \'function\') {\n' +
            'return cb(\'node-uuid module is not defined\');\n' +
        '}\n' +
        'if (typeof (foo) !== \'string\') {\n' +
            'return cb(\'sandbox value is not defined\');\n' +
        '}\n' +
        'if (typeof (bool) !== \'boolean\') {\n' +
            'return cb(\'sandbox value is not defined\');\n' +
        '}\n' +
        'if (typeof (aNumber) !== \'number\') {\n' +
            'return cb(\'sandbox value is not defined\');\n' +
        '}\n' +
        'return cb(null);\n' +
    '}';

    job.chain.push(task);

    var wf_task_runner = new WorkflowTaskRunner({
        job: job,
        task: task,
        sandbox: sandbox
    });

    t.ok(wf_task_runner.uuid);
    t.equal(typeof (wf_task_runner.body), 'function');

    wf_task_runner.runTask(function (msg) {
        t.ok(msg.result);
        t.ifError(msg.error, 'task error');
        t.ok(msg.job);
        t.equal(msg.cmd, 'run');
        t.equal(msg.task_name, task.name);
        t.end();
    });

});


test('a task which succeeds on 2nd retry', function (t) {
    task.body = function (job, cb) {
        if (!job.foo) {
            job.foo = true;
            return cb('Foo was not defined');
        }
        return cb(null);
    }.toString();
    task.retry = 2;
    job.chain.push(task);

    var wf_task_runner = new WorkflowTaskRunner({
        job: job,
        task: task
    });

    t.ok(wf_task_runner.uuid);
    t.equal(typeof (wf_task_runner.body), 'function');

    wf_task_runner.runTask(function (msg) {
        t.ok(msg.result);
        t.ifError(msg.error, 'task error');
        t.ok(msg.job);
        t.ok(msg.job.foo);
        t.equal(msg.cmd, 'run');
        t.equal(msg.task_name, task.name);
        t.end();
    });
});


test('a task which fails and has no "fallback"', function (t) {
    task.body = function (job, cb) {
        return cb('Task body error');
    }.toString();

    job.chain.push(task);

    var wf_task_runner = new WorkflowTaskRunner({
        job: job,
        task: task
    });

    t.ok(wf_task_runner.uuid);
    t.equal(typeof (wf_task_runner.body), 'function');

    wf_task_runner.runTask(function (msg) {
        t.ifError(msg.result);
        t.equal(msg.error, 'Task body error', 'task error');
        t.ok(msg.job);
        t.equal(msg.cmd, 'error');
        t.equal(msg.task_name, task.name);
        t.end();
    });
});


test('a task which fails and succeeds "fallback"', function (t) {
    task.fallback = function (err, job, cb) {
        job.the_err = err;
        return cb(null);
    }.toString();

    job.chain.push(task);

    var wf_task_runner = new WorkflowTaskRunner({
        job: job,
        task: task
    });

    t.ok(wf_task_runner.uuid);
    t.equal(typeof (wf_task_runner.body), 'function');
    t.equal(typeof (wf_task_runner.fallback), 'function');

    wf_task_runner.runTask(function (msg) {
        t.ok(msg.result);
        t.ifError(msg.error, 'task error');
        t.ok(msg.job);
        t.equal(msg.job.the_err, 'Task body error');
        t.equal(msg.cmd, 'run');
        t.end();
    });
});


test('a task which fails and "fallback" fails too', function (t) {
    task.fallback = function (err, job, cb) {
        return cb('fallback error');
    }.toString();

    job.chain.push(task);

    var wf_task_runner = new WorkflowTaskRunner({
        job: job,
        task: task
    });

    t.ok(wf_task_runner.uuid);
    t.equal(typeof (wf_task_runner.body), 'function');
    t.equal(typeof (wf_task_runner.fallback), 'function');

    wf_task_runner.runTask(function (msg) {
        t.ifError(msg.result);
        t.equal(msg.error, 'fallback error', 'task error');
        t.ok(msg.job);
        t.equal(msg.cmd, 'error');
        t.end();
    });
});


test('a task which fails after two retries and has no "fallback"',
    function (t) {
        task.body = function (job, cb) {
            if (!job.bar) {
                job.bar = true;
                return cb('Bar was not defined');
            } else if (!job.baz) {
                job.baz = true;
                return cb('Baz was not defined');
            }
            // Should not be called
            return cb(null);
        }.toString();
        task.fallback = null;

        job.chain.push(task);

        var wf_task_runner = new WorkflowTaskRunner({
            job: job,
            task: task
        });

        t.ok(wf_task_runner.uuid);
        t.equal(typeof (wf_task_runner.body), 'function');

        wf_task_runner.runTask(function (msg) {
            t.ifError(msg.result);
            t.equal(msg.error, 'Baz was not defined', 'task error');
            t.ok(msg.job, 'job ok');
            t.ok(msg.job.bar, 'job.bar ok');
            t.ok(msg.job.baz, 'job.baz ok');
            t.equal(msg.cmd, 'error', 'job cmd ok');
            t.end();
        });
    });


test('a task which time out and succeeds "fallback"', function (t) {
    task.body = function (job, cb) {
        setTimeout(function () {
            // Should not be called:
            return cb('Error within timeout');
        }, 1050);
    }.toString();
    task.fallback = function (err, job, cb) {
        job.the_err = err;
        return cb(null);
    }.toString();
    task.timeout = 1;
    task.retry = 1;

    job.chain.push(task);

    var wf_task_runner = new WorkflowTaskRunner({
        job: job,
        task: task
    });

    t.ok(wf_task_runner.uuid);
    t.equal(typeof (wf_task_runner.body), 'function');
    t.equal(typeof (wf_task_runner.fallback), 'function');

    t.equal(wf_task_runner.timeout, 1000);

    wf_task_runner.runTask(function (msg) {
        t.ok(msg.result);
        t.ifError(msg.error, 'task error');
        t.ok(msg.job);
        t.equal(msg.job.the_err, 'task timeout error');
        t.equal(msg.cmd, 'run');
        t.end();
    });
});


test('a task which times out and fallback does too', function (t) {
    task.body = function (job, cb) {
        job.timer = 'Timeout set';
        setTimeout(function () {
            // Should not be called:
            return cb(null);
        }, 1050);
    }.toString();
    task.retry = 1;
    task.fallback = function (err, job, cb) {
        job.fbtimer = 'Fallback timeout set';
        setTimeout(function () {
            // Should not be called:
            return cb(null);
        }, 1025);
    }.toString();

    job.chain.push(task);

    var wf_task_runner = new WorkflowTaskRunner({
        job: job,
        task: task,
        trace: false
    });

    t.ok(wf_task_runner.uuid, 'uuid ok');
    t.equal(typeof (wf_task_runner.body), 'function', 'body ok');
    t.equal(typeof (wf_task_runner.fallback), 'function', 'fallback ok');
    t.equal(wf_task_runner.timeout, 1000, 'timeout ok');

    wf_task_runner.runTask(function (msg) {
        t.ok(msg.error, 'task error');
        t.equal(msg.error, 'task timeout error', 'task timeout error');
        t.ifError(msg.result, 'task result');
        t.ok(msg.job, 'job ok');
        t.equal(msg.cmd, 'error', 'cmd ok');
        t.end();
    });
});

test('a task which succeeds and re-queues the workflow', function (t) {
    task.body = function (job, cb) {
        return cb('queue');
    }.toString();
    task.fallback = null;
    task.retry = 1;

    job.chain.push(task);

    var wf_task_runner = new WorkflowTaskRunner({
        job: job,
        task: task
    });

    t.ok(wf_task_runner.uuid);
    t.equal(typeof (wf_task_runner.body), 'function');

    wf_task_runner.runTask(function (msg) {
        t.ok(msg.result);
        t.ok(msg.error, 'task error');
        t.ok(msg.job);
        t.equal(msg.cmd, 'queue');
        t.end();
    });

});


test('a task which times out and has no fallback', function (t) {
    task.body = function (job, cb) {
        job.timer = 'Timeout set';
        setTimeout(function () {
            // Should not be called:
            return cb(null);
        }, 1050);
    }.toString();
    task.retry = 1;
    task.fallback = null;
    job.chain.push(task);

    var wf_task_runner = new WorkflowTaskRunner({
        job: job,
        task: task,
        trace: false
    });

    t.ok(wf_task_runner.uuid, 'uuid ok');
    t.equal(typeof (wf_task_runner.body), 'function', 'body ok');
    t.equal(wf_task_runner.timeout, 1000, 'timeout ok');

    wf_task_runner.runTask(function (msg) {
        t.ok(msg.error, 'task error');
        t.equal(msg.error, 'task timeout error', 'task timeout error');
        t.ifError(msg.result, 'task result');
        t.ok(msg.job, 'job ok');
        t.equal(msg.cmd, 'error', 'cmd ok');
        t.end();
    });

});


test('a task which timeout and is canceled', function (t) {
    task.body = function (job, cb) {
        job.timer = 'Timeout set';
        setTimeout(function () {
            // Should not be called:
            return cb(null);
        }, 1550);
    }.toString();
    task.retry = 2;
    task.fallback = null;
    job.chain.push(task);

    var wf_task_runner = new WorkflowTaskRunner({
        job: job,
        task: task,
        trace: false
    });

    t.ok(wf_task_runner.uuid, 'uuid ok');
    t.equal(typeof (wf_task_runner.body), 'function', 'body ok');
    t.equal(wf_task_runner.timeout, 1000, 'timeout ok');

    setTimeout(function () {
        wf_task_runner.canceled = true;
    }, 750);

    wf_task_runner.runTask(function (msg) {
        t.ok(msg.error, 'task error');
        t.equal(msg.error, 'cancel', 'task timeout error');
        t.ifError(msg.result, 'task result');
        t.ok(msg.job, 'job ok');
        t.equal(msg.cmd, 'cancel', 'cmd ok');
        t.end();
    });

});


test('a task which fails and is canceled', function (t) {
    task.body = function (job, cb) {
        setTimeout(function () {
            return cb('Task body error');
        }, 500);
    }.toString();

    task.fallback = function (err, job, cb) {
        job.the_err = err;
        return cb(null);
    }.toString();

    task.retry = 1;

    job.chain.push(task);

    var wf_task_runner = new WorkflowTaskRunner({
        job: job,
        task: task
    });

    t.ok(wf_task_runner.uuid);
    t.equal(typeof (wf_task_runner.body), 'function');
    t.equal(typeof (wf_task_runner.fallback), 'function');

    setTimeout(function () {
        wf_task_runner.canceled = true;
    }, 350);

    wf_task_runner.runTask(function (msg) {
        t.ok(msg.error, 'task error');
        t.equal(msg.error, 'cancel', 'task timeout error');
        t.ifError(msg.result, 'task result');
        t.ok(msg.job, 'job ok');
        t.equal(msg.cmd, 'cancel', 'cmd ok');
        t.end();
    });

});
