from airflow.utils.decorators import apply_defaults
from airflow.operators.email_operator import EmailOperator

from event_plugins.common.storage.event_message import EventMessageCRUD


class BaseStatusEmailOperator(EmailOperator):

    ui_color = '#7FB0B0'

    plugin_name = 'base'

    # if using airflow cli command to run task, dag_run would be None
    default_subject = "[Failed] {{ dag_run.dag_id }} {{ execution_date }}"

    @apply_defaults
    def __init__(self,
                status_path,
                subject=None,
                html_content=None,
                threshold=None,
                *args, **kwargs):
        super(BaseStatusEmailOperator, self).__init__(subject=subject,
                                                       html_content=html_content,
                                                       *args, **kwargs)
        self.db_handler = MessageRecordCRUD(status_path, self.plugin_name)
        self.threshold = threshold
        if not self.subject:
            self.subject=self.default_subject
        if not self.html_content:
            self.html_content = self.generate_html()

    def generate_html(self):
        shelve_db_html = self.db_handler.tabulate_data(threshold=self.threshold, tablefmt='html')
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
