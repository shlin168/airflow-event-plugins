import os

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from airflow.settings import Session

from event_plugins.common.config import read_config


# read storage setting from config file
# set environment variable for the location of config file
STORAGE_CONF_FILE = os.environ.get("AIRFLOW_EVENT_PLUGINS_CONFIG")
# else use default.conf
CONF_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_STORAGE_CONF_FILE = os.path.join(CONF_DIR, "default.cfg")

if STORAGE_CONF_FILE is not None:
    STORAGE_CONF = read_config(STORAGE_CONF_FILE)
else:
    STORAGE_CONF = read_config(DEFAULT_STORAGE_CONF_FILE)

USE_AIRFLOW_DATABASE = None


def get_session(sql_alchemy_conn=None):
    if sql_alchemy_conn is None:
        sql_alchemy_conn = STORAGE_CONF.get("Storage", "sql_alchemy_conn")
    if sql_alchemy_conn != '':
        USE_AIRFLOW_DATABASE = False
        engine = create_engine(sql_alchemy_conn)
        return sessionmaker(bind=engine)()
    else:
        USE_AIRFLOW_DATABASE = True
        return Session
