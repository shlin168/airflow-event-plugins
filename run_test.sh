#!/bin/sh
export SERVICE_HOME="$(cd "`dirname "$0"`"; pwd)"

export PYTHONPATH="${PYTHONPATH}:${SERVICE_HOME}/plugins"

export AIRFLOW_HOME=$SERVICE_HOME/test_plugins/test_data
export AIRFLOW_EVENT_PLUGINS_CONFIG=$SERVICE_HOME/test_plugins/test_data/test_event_plugins.cfg

py.test $@
