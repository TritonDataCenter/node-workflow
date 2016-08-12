# Tasks Workflows Orchestration API and Runners.

This repository is part of the Joyent Triton project. See the [contribution
guidelines](https://github.com/joyent/triton/blob/master/CONTRIBUTING.md) --
*Triton does not use GitHub PRs* -- and general documentation at the main
[Triton project](https://github.com/joyent/triton) page.


- Repository: <git://github.com/joyent/node-workflow.git>
- Browsing: <https://github.com/joyent/node-workflow>
- Who: [Pedro Palazón Candel](https://github.com/kusor), [Trent Mick](https://github.com/trentm), [Mark Cavage](https://github.com/mcavage), [Josh Wilsdon](https://github.com/joshwilsdon), [Rob Gulewich](https://github.com/rgulewich), [Andrés Rodríguez](https://github.com/cachafla), [Fred Kuo](https://github.com/fkuo).
- Docs: <http://joyent.github.io/node-workflow/>
- Tickets/bugs: <https://github.com/joyent/node-workflow/issues>


# Installation

    npm install wf

# Overview

If you are building a completely new system composed of many discrete API
applications, each of them with a clearly defined area of responsibility, or
if you are trying to assemble a collaboration channel between a heterogeneous
set of unrelated API applications, you need a means to orchestrate interactions
between these applications.

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

## wf

This package provides a way to define re-usable `workflows` using JSON and run
concrete `jobs` with specific `targets` and `parameters` based on such
`workflows`.

### Workflow Runners

In order to execute `jobs`, at least one `WorkflowRunner` must be up and ready
to take jobs. An arbitrary number of `runners` can be used on any set of hosts;
their configuration must match.

An example `WorkflowRunner` is provided with the package and can be started
with:

    ./bin/workflow-runner path/to/config.json

(The `test` directory contains the file `config.json.sample` which can be
used as reference).

You can create `workflows` and `jobs` either by using the provided REST API(s),
or by embedding this module's API into your own system(s). The former will be
easier to get up and running, but you should use the latter when:

- You want to use the Worflow API in a (node.js) application that is not the
  bundled [REST API](http://joyent.github.io/node-workflow/workflowapi.html).
- You want to use a different backend storage system, or otherwise change the
  assumptions of the bundled REST API.

The package also provides a binary file to run the `WorkflowAPI` using the
same configuration file we pass to our `WorkflowRunner`:

    ./bin/workflow-api path/to/config.json

See demo section below for more details about both approaches.

# Development

## Clone the repo and build the deps:

    git clone git://github.com/joyent/node-workflow.git
    cd node-workflow
    make all

Note `make all` will setup all the required dependencies, node modules and run
`make check` and `make test`. In order to just setup node modules, `make setup`
is enough.

To run the Workflow API server:

    ./bin/workflow-api path/to/config.json

To run a Job Runner:

    ./bin/workflow-runner path/to/config.json

Note that it's fine to run more than one Runner, either on the same or different
machines, so long as they have access to the Redis Server.

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

If you've made a change that does not affect source code files, but (for
example) only docs, you can skip this hook by passing the option `--no-verify`
to the `git commit` command.

# Demo

The [workflow-example](https://github.com/kusor/node-workflow-example)
repository contains everything needed to illustrate:

- An example config file `config.json.sample` which should be renamed to 
  `config.json`, and modified to properly match your local environment.

Remember that, in order to process any `job` the `workflow-runner` needs to be
initialized pointing to the aforementioned configuration file:

    ./node_modules/.bin/workflow-runner config.json

Also, in order to be able to run the API-based example mentioned below, the
`workflow-api` HTTP server needs to be up and running too:

    ./node_modules/.bin/workflow-api config.json

Contents for the other files within the [workflow-example](https://github.com/kusor/node-workflow-example)
repository are:

- An example of how to use node-workflow as a node module in order to create
  workflows, queue jobs and wait for the results. See `module.js`.
- An example of how to achieve the same goal using the Workflow API instead of
  the node module. See `api.js`.
- Both examples share the same workflow definition, contained in the file
  `shared-workflow.js`. The beginning of the aforementioned files
  can be useful to understand the differences when trying to create a workflow
  using these different approaches.
- Finally, this directory also contains the file `node.js`, which does
  exactly the same thing as the workflow/job does -- create and star a gist
  using your github's username and password -- but straight from node.js. This
  file is useful in order to understand the differences between writing code
  to be executed by node.js directly, and using it to create workflows and the
  associated tasks. Remember, code within tasks runs sandboxed using
  [Node's VM API](http://nodejs.org/docs/latest/api/vm.html) and that tasks
  are totally independent.

See also `example.js` for more options when defining workflows and the different
possibilities for task fallbacks, retries, timeouts, ...

# Repository

    deps/           Git submodules and/or commited 3rd-party deps.
                    See "node_modules/" for node.js deps.
    docs/           Project docs (restdown)
    lib/            Source files.
    node_modules/   Node.js deps, either populated at build time or commited.
    pkg/            Package lifecycle scripts
    test/           Test suite (using node-tap)
    tools/          Miscellaneous dev/upgrade/deployment tools and data.
    Makefile
    package.json    npm module info
    README.md

# TODO

See https://github.com/joyent/node-workflow/issues.

# LICENSE

The MIT License (MIT) Copyright (c) 2016 Pedro Palazón Candel

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

