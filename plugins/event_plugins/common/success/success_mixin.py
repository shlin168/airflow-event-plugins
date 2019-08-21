from airflow.models import TaskInstance
from airflow.utils import timezone
from airflow.utils.db import provide_session
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.state import State


class SuccessMixin(LoggingMixin):
    @provide_session
    def success(self, dag_run, execution_date, tasks, session=None):
        """
        Sets tasks instances to skipped from the same dag run.
        :param dag_run: the DagRun for which to set the tasks to success
        :param execution_date: execution_date
        :param tasks: tasks to mark success (not task_ids)
        :param session: db session to use
        """
        if not tasks:
            return

        task_ids = [d.task_id for d in tasks]
        # use airflow timezone to get now here
        now = timezone.utcnow()

        if dag_run:
            session.query(TaskInstance).filter(
                TaskInstance.dag_id == dag_run.dag_id,
                TaskInstance.execution_date == dag_run.execution_date,
                TaskInstance.task_id.in_(task_ids)
            ).update({TaskInstance.state: State.SUCCESS,
                      TaskInstance.start_date: now,
                      TaskInstance.end_date: now},
                     synchronize_session=False)
            session.commit()
        else:
            assert execution_date is not None, "Execution date is None and no dag run"

            self.log.warning("No DAG RUN present this should not happen")
            # this is defensive against dag runs that are not complete
            for task in tasks:
                ti = TaskInstance(task, execution_date=execution_date)
                ti.state = State.SKIPPED
                ti.start_date = now
                ti.end_date = now
                session.merge(ti)

            session.commit()
