from airflow.utils.decorators import apply_defaults
from airflow.operators.email_operator import EmailOperator

from event_plugins.common.storage.db import get_session, USE_AIRFLOW_DATABASE
from event_plugins.common.storage.event_message import EventMessageCRUD


class BaseStatusEmailOperator(EmailOperator):

    ui_color = '#7FB0B0'

    source_type = 'base'

    # if using airflow cli command to run task, dag_run would be None
    default_subject = "[Failed] {{ dag_run.dag_id }} {{ execution_date }}"

    @apply_defaults
    def __init__(self,
                sensor_name,
                subject=None,
                html_content=None,
                threshold=None,
                *args, **kwargs):
        super(BaseStatusEmailOperator, self).__init__(subject=subject,
                                                       html_content=html_content,
                                                       *args, **kwargs)
        self.set_db_handler(sensor_name)
        self.threshold = threshold
        if not self.subject:
            self.subject=self.default_subject
        if not self.html_content:
            self.html_content = self.generate_html()

    def set_db_handler(self, sensor_name):
        with get_session() as session:
            self.db_handler = EventMessageCRUD(self.source_type, sensor_name, session)

    def generate_html(self):
        shelve_db_html = self.db_handler.tabulate_data(threshold=self.threshold, tablefmt='html')
        self.close_connection()
        # use old method to compose html string since using 'format' will lead to
        # jinja template confliction
        return """
            <html>
                <body>
                    <h1 style="color: #5e9ca0;">Event-Trigger Sensor Information</h1>
                    There're some messages not received before timeout<hr>
                    <h2 style="color: #2e6c80;">DAG ID: <span style="color: #cc0000;">{{ dag_run.dag_id }}</span></h2>
                    <h3 style="color: #2e6c80;">Execution time: <span style="color: #cc0000;">{{ dag_run.execution_date }}</span></h3>
                    <h3 style="color: #2e6c80;">Schedule interval: <span style="color: #cc0000;">{{ dag.schedule_interval }}</span></h3>
                    <h4>The status of sensor before DAG timeout</h4>
                    %s
                </body>
            </html>
        """ % (shelve_db_html)

    def close_connection(self):
        # close db connection if not using airflow database to store messages status
        if USE_AIRFLOW_DATABASE is False:
            self.db_handler.session.remove()
