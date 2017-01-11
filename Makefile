#
# Copyright (c) 2012, Joyent, Inc. All rights reserved.
# Copyright (c) 2017, Joyent, Inc.
#

#
# Tools
#
NPM		:= $(shell which npm)
TAP		:= ./node_modules/.bin/tap
RESTDOWN_FLAGS  := -b deps/restdown/brand/spartan

#
# Files
#
DOC_FILES	 = index.md motivation.md workflowapi.md
JS_FILES	:= $(shell ls *.js) $(shell find lib test -name '*.js')
JSL_CONF_NODE	 = tools/jsl.node.conf
JSL_FILES_NODE   = $(JS_FILES)
JSSTYLE_FILES	 = $(JS_FILES)
JSSTYLE_FLAGS    = -o indent=4,doxygen,unparenthesized-return=0
# SMF_MANIFESTS	 = smf/manifests/bapi.xml

#
# Repo-specific targets
#
.PHONY: all
all: test check

.PHONY: setup
setup: $(NPM)
	$(NPM) install

.PHONY: test
test: setup nofork $(TAP)
	TAP_TIMEOUT=80 $(TAP) test/*.test.js

.PHONY: nofork
nofork: setup $(TAP)
	TAP_TIMEOUT=80 TEST_CONFIG_FILE=config.nofork.sample $(TAP) test/runner.test.js

.PHONY: coverage
coverage: $(TAP)
	TAP_COV=1 $(TAP) test/*.test.js

include ./Makefile.deps
include ./Makefile.targ
