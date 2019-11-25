# -*- coding: UTF-8 -*-
from __future__ import print_function

from functools import wraps

from event_plugins.common.jinja import Jinja


class MsgRenderUtils(object):

    @staticmethod
    def get_render_dict(ins, render_match_keys):
        ''' render values using method in ins
            Args:
                ins(object): the class instance that define rendering funcs
                render_match_keys(list): the values of keys that need to be rendered
            Returns:
                render_result(list): replace values in render_match_keys that need to be rendered
                E.g., if render_match_keys = [('partition_values', {'yyyymm': '_get_exec_partition'})]
                         render_result = [('partition_values', {'yyyymm': '201903'})]
        '''
        render_result = list()
        for key, pre_rdict in render_match_keys:
            post_rdict = dict()
            for rkey in pre_rdict:
                if not hasattr(ins, pre_rdict[rkey]):
                    raise NotImplementedError("""Need to implement {} function which return render result""".format(pre_rdict[rkey]))
                rval = getattr(ins, pre_rdict[rkey])()
                post_rdict.update({rkey: rval})
            render_result.append((key, post_rdict))
        return render_result

    @staticmethod
    def render(input_string, render_dict):
        return Jinja().render(input_string, **render_dict)


def render_func(key):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            # print("render value of {} using {}".format(key, func.__name__))
            return func(*args, **kwargs)
        return wrapper
    return decorator
