from jinja2 import Environment, BaseLoader
from datetime import datetime
from dateutil.relativedelta import relativedelta


class Jinja:

    @property
    def env(self):
        try:
            return self._env
        except AttributeError:
            self._env = Environment(loader=BaseLoader())
            self.set_filters()
            return self._env

    def set_filters(self):
        self.env.filters['dt.format'] = CustomFilters.dt_format
        self.env.filters['dt.add_month'] = CustomFilters.dt_add_month
        self.env.filters['dt.add_day'] = CustomFilters.dt_add_day

    def render(self, base, **kwargs):
        template = self.env.from_string(base)
        return template.render(**kwargs)


class CustomFilters:

    @staticmethod
    def dt_format(value, format='%Y-%m-%d'):
        assert isinstance(value, datetime), 'value need to be datetime istance'
        return value.strftime(format)

    @staticmethod
    def dt_add_month(value, trace_num=0, format='%Y-%m'):
        assert isinstance(value, datetime), 'value need to be datetime istance'
        return (value + relativedelta(months=trace_num)).strftime(format)

    @staticmethod
    def dt_add_day(value, trace_num=0, format='%Y-%m-%d'):
        assert isinstance(value, datetime), 'value need to be datetime istance'
        return (value + relativedelta(days=trace_num)).strftime(format)
