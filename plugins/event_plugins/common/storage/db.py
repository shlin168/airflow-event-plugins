import contextlib
import os

from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker

from airflow.models.base import Base
from airflow.settings import Session, engine as airflow_engine

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
    session = None
    if sql_alchemy_conn is None:
        sql_alchemy_conn = STORAGE_CONF.get("Storage", "sql_alchemy_conn")
    if sql_alchemy_conn != '':
        USE_AIRFLOW_DATABASE = False
        engine = create_engine(sql_alchemy_conn)
        create_table_if_not_exist(engine)
        session = scoped_session(
            sessionmaker(autocommit=False,
                         autoflush=False,
                         bind=engine,
                         expire_on_commit=False))
    else:
        USE_AIRFLOW_DATABASE = True
        create_table_if_not_exist(airflow_engine)
        session = Session
    return session


def db_commit(func):
    def _db_commit(*args, **kwargs):
        try:
            func(*args, **kwargs)
            session = args[0].session
            session.commit()
        except:
            session.rollback()
            raise
        finally:
            session.close()
    return _db_commit


def create_table_if_not_exist(engine):
    if STORAGE_CONF.getboolean("Storage", "create_table_if_not_exist") is True:
        Base.metadata.create_all(engine)
