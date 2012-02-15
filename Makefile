#
# Copyright (c) 2012, Joyent, Inc. All rights reserved.
#

#
# Tools
#
NPM		:= npm
TAP		:= ./node_modules/.bin/tap

#
# Files
#
DOC_FILES	 = index.restdown workflowapi.restdown
JS_FILES	:= $(shell ls *.js) $(shell find lib test -name '*.js')
JSL_CONF_NODE	 = tools/jsl.node.conf
JSL_FILES_NODE   = $(JS_FILES)
JSSTYLE_FILES	 = $(JS_FILES)
JSSTYLE_FLAGS    = -o indent=2,doxygen,unparenthesized-return=0
# SMF_MANIFESTS	 = smf/manifests/bapi.xml

#
# Repo-specific targets
#
.PHONY: all
all:
	$(NPM) rebuild

.PHONY: setup
setup: $(NPM)
	$(NPM) install

.PHONY: test
test: setup $(TAP)
	$(TAP) test/*.test.js

.PHONY: coverage
coverage: $(TAP)
	TAP_COV=1 $(TAP) test/*.test.js

include ./Makefile.deps
include ./Makefile.targ

