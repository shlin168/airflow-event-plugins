#!/bin/sh
export SERVICE_HOME="$(cd "`dirname "$0"`"; pwd)"

PYTHONPATH="${PYTHONPATH}:${SERVICE_HOME}/plugins"

py.test $@
