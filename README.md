# Tasks Workflows Orchestration API and Runners.

- Repository: <git://github.com/kusor/node-workflow.git>
- Browsing: <https://github.com/kusor/node-workflow>
- Who: Pedro Palazón Candel
- Docs: <https://...>
- Tickets/bugs: <https://github.com/kusor/node-workflow/issues>

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

# Repository

    deps/           Git submodules and/or commited 3rd-party deps.
                    See "node_modules/" for node.js deps.
    docs/           Project docs (restdown)
    lib/            Source files.
    node_modules/   Node.js deps, either populated at build time or commited.
    pkg/            Package lifecycle scripts
    smf/manifests   SMF manifests
    smf/methods     SMF method scripts
    test/           Test suite (using node-tap)
    tools/          Miscellaneous dev/upgrade/deployment tools and data.
    Makefile
    package.json    npm module info
    README.md


# Development

Pre-requirements:

- Working Redis Server. (Version 2.4.+).

# Clone the repo and build the deps:

    git clone git://github.com/kusor/node-workflow.git
    cd node-workflow
    make all

Note `make all` will setup all the required dependencies, node modules and run
`make check` and `make test`. In order to just setup node modules, `make setup`
is enough.

To run the Workflow API server:

    node lib/api.js

To run a Job Runner:

    node lib/runner.js

Note that it's perfectly fine to run more than one Runner, either on the same
or different machines, as far as they have access to Redis Server.

# Testing

    make test

# Pre-commit git hook

In order to run `make prepush` before every commit, add the following to a file
called `.git/hooks/pre-commit` and `chmod +x` it:

    #!/bin/sh
    # Run make prepush before allow commit

    set -e

    make prepush

    exit 0

Obviously, if you've made a change not affecting source code files but, for
example only docs, you can skip this hook by passing the option `--no-verify`
to the `git commit` command.

# Demo

The directory `examples` contain everything needed to illustrate:

- An example config file - `examples/config.json.sample` which should be
  renamed to `examples/config.json` and modified to properly match your local
  environment.
- How to run the Workflow API with appropriated configuration for your env.
  See `examples/wf-api.js`.
- How to run the Workflow Runners with appropriated config. See
  `examples/wf-runner.js`.
- An example of how to use node-workflow as a node module in order to create
  workflows, queue jobs and wait for the results. See `examples/module.js`.
- Also, an example of how to achieve same goal using Workflow API instead of
  the node module. See `examples/api.js`.
- Both examples share the same workflow definition, contained at the file
  `examples/shared-workflow.js`. The beginning of the aforementioned files
  can be useful to understand the differences when trying to create a workflow
  using these different approaches.
- Finally, this directory also contains a file `examples/node.js` which does
  exactly the same thing than the workflow/job does - create and star a gist
  using your github's username and password - but straight from NodeJS. This
  file is useful in order to understand the differences between writing code
  to be executed by NodeJS directly, and using it to create workflows and the
  associated tasks. Remember code withing tasks runs sandboxed using
  [Node's VM API](http://nodejs.org/docs/latest/api/vm.html) and that tasks
  are totally independent, and run each one on its own child process.

Also, see `example.js` for more options when defining workflows and the
different possibilities for tasks fallbacks, retries, timeouts ...

# TODO

See https://github.com/kusor/node-workflow/issues.

# LICENSE

The MIT License (MIT) Copyright (c) 2012 Pedro Palazón Candel

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

