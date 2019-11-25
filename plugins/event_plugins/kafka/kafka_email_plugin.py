from tabulate import tabulate

from airflow.utils.decorators import apply_defaults

from event_plugins.base.base_email_plugin import BaseStatusEmailOperator
from event_plugins.kafka.comsume.topic import topic_map, topic_factory


class KafkaStatusEmailOperator(BaseStatusEmailOperator):

    ui_color = '#7FB0B0'

    plugin_name = 'kafka'

    # if using airflow cli command to run task, dag_run would be None
    default_subject = "[Failed] {{ dag_run.dag_id }} {{ execution_date }}"

    @apply_defaults
    def __init__(self, *args, **kwargs):
        super(KafkaStatusEmailOperator, self).__init__(*args, **kwargs)

    def set_topic_cols_map(self):
        def get_match_cols(topic):
            # input None to get the match_keys and render_match_keys Message class
            topic_obj = topic_factory(topic).msg_handler(None)
            return topic_obj.match_keys + [k[0] for k in topic_obj.render_match_keys]

        self.topic_cols_map = dict()
        for topic in topic_map:
            self.topic_cols_map.update({ topic: get_match_cols(topic) })

    def get_unreceived_msgs(self):
        ''' Get unreceived messages and change to html format
            Returns:
                unreceived_dict(dict):
                    { topic_name: <tabluate html format data> }
        '''
        self.set_topic_cols_map()
        unreceived_dict = dict()
        for msg in self.db_handler.get_unreceived_msgs():
            if msg['topic'] not in unreceived_dict:
                unreceived_dict.update(
                    { msg['topic']: [[msg.get(key) for key in self.topic_cols_map[msg['topic']]]] })
            else:
                unreceived_dict[msg['topic']].append(
                    [msg.get(key) for key in self.topic_cols_map[msg['topic']]])
        for topic in unreceived_dict:
            unreceived_dict[topic] = tabulate(unreceived_dict[topic],
                headers=self.topic_cols_map[topic], tablefmt='html')
        return unreceived_dict

    def generate_html(self):
        shelve_db_html = self.db_handler.tabulate_data(threshold=self.threshold, tablefmt='html')
        unreceived_html = ""
        unreceived_dict = self.get_unreceived_msgs()
        for topic in unreceived_dict:
            unreceived_html += """<h4 style="color: #558B2F"> topic: {t} </h4>{tbl}""".format(
                t=topic, tbl=unreceived_dict[topic])

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
                    <h3 style="color: #2e6c80;">Unreceive Wanted Messages:</h3>
                    %s<hr>
                    <h4>The status of sensor before DAG timeout</h4>
                    %s
                </body>
            </html>
        """ % (unreceived_html, shelve_db_html)
