import pytest
import mock

from event_plugins.common.schedule.time_utils import TimeUtils
from event_plugins.common.schedule.timeout import TaskTimeout


@pytest.fixture()
def schedule_context():
    # when the job is triggered at 2019/4/23 01:00:00
    schedule_context = {
        'execution_date': TimeUtils().datetime(2019, 4, 22, 1, 0, 0),
        'next_execution_date': TimeUtils().datetime(2019, 4, 23, 1, 0, 0)
    }
    return schedule_context

@pytest.fixture()
def manual_context():
    # when the job is triggered at 2019/4/22 01:00:00
    manual_context = {
        'execution_date': TimeUtils().datetime(2019, 4, 22, 1, 0, 0),
        'next_execution_date': TimeUtils().datetime(2019, 4, 22, 1, 0, 0)
    }
    return manual_context

def patch_now(mocker, now):
    mocker.patch.object(TimeUtils, 'get_now', return_value=now)

poke_interval = 60


class TestTaskTimeout:

    @pytest.mark.usefixtures("schedule_context", "manual_context")
    def test_set_task_type(self, schedule_context, manual_context):
        handler = TaskTimeout(schedule_context, poke_interval, 60)
        assert handler.task_type == 'scheduled'
        assert handler.execution_date == TimeUtils().datetime(2019, 4, 23, 1, 0, 0)

        mhandler = TaskTimeout(manual_context, poke_interval, 60)
        assert mhandler.task_type == 'manual'
        assert mhandler.execution_date == TimeUtils().datetime(2019, 4, 22, 1, 0, 0)


    @pytest.mark.usefixtures("schedule_context")
    @pytest.mark.parametrize("timeout, expected", [
        (60*60*24, 'seconds'),
        ('* 22 * * *', 'crontab'),
        (None, None)
    ])
    def test_set_timeout_mode(self, schedule_context, timeout, expected):
        handler = TaskTimeout(schedule_context, poke_interval, timeout)
        assert handler.timeout_mode == expected


    @pytest.mark.usefixtures("schedule_context")
    def test_set_timeout_mode_invalid(self, schedule_context):
        timout = 'invalid crontab'
        with pytest.raises(Exception):
            assert TaskTimeout(schedule_context, poke_interval, timeout)
        timeout = 20.0
        with pytest.raises(Exception):
            assert TaskTimeout(schedule_context, poke_interval, timeout)


    @pytest.mark.usefixtures("schedule_context")
    @pytest.mark.parametrize("timeout, expected", [
        (60*60*24, TimeUtils().datetime(2019, 4, 24, 1, 0, 0)),
        (60*60*25, TimeUtils().datetime(2019, 4, 24, 1, 0, 0)),
        ('* 22 * * *', TimeUtils().datetime(2019, 4, 23, 22, 0, 0))
    ])
    def test_set_timeout_dt(self, schedule_context, timeout, expected):
        handler = TaskTimeout(schedule_context, poke_interval, timeout)
        handler.set_timeout_dt()
        assert handler.timeout_dt == expected


    #@mock.patch.object('event_plugins.common.schedule.time_utils.TimeUtils', 'get_now')
    @pytest.mark.usefixtures("manual_context")
    def test_manual_is_timeout(self, manual_context, mocker):
        timeout = 60*60*24
        handler = TaskTimeout(manual_context, poke_interval, timeout)
        assert handler.timeout_dt == TimeUtils().datetime(2019, 4, 23, 1, 0, 0)

        #######################################
        #  [Time Changed] 2019/04/22 00:08:00 #
        #######################################
        # all timeout of tests should return false
        patch_now(mocker, TimeUtils().datetime(2019, 4, 22, 8, 0, 0))
        assert handler.is_timeout() == False

        #######################################
        #  [Time Changed] 2019/04/23 00:59:10 #
        #######################################
        # is_timeout() return False
        # next poke: 2019/04/23 22:00:10 > 2019/04/23 22:00:10
        # so is_next_poke_timeout() return True, and set execute_last_poke_after_secs
        patch_now(mocker, TimeUtils().datetime(2019, 4, 23, 0, 59, 10))
        assert poke_interval == 60
        assert handler.is_timeout() == False
        assert handler.last_poke_offset == 30
        assert handler.is_next_poke_timeout() == True
        assert handler.execute_last_poke_after_secs == 20

        #######################################
        #  [Time Changed] 2019/04/23 00:59:30 #
        #######################################
        # enumlate that execute_last_poke_after_secs is not set,
        # and the task is triggered at 2019/04/23 21:59:30
        handler.execute_last_poke_after_secs = None
        patch_now(mocker, TimeUtils().datetime(2019, 4, 23, 0, 59, 30))
        assert handler.is_timeout() == False
        assert handler.last_poke_offset == 30
        assert handler.is_next_poke_timeout() == True
        # only 30 secs before timeout which <= last_poke_offset
        # it won't set to perform the last poke
        assert handler.execute_last_poke_after_secs is None

        #######################################
        #  [Time Changed] 2019/04/23 01:00:00 #
        #######################################
        patch_now(mocker, TimeUtils().datetime(2019, 4, 23, 1, 0, 0))
        assert handler.is_timeout() == True
        assert handler.is_next_poke_timeout() == True


    @pytest.mark.usefixtures("manual_context")
    def test_manual_is_timeout_None(self, manual_context):
        timeout = None
        with pytest.raises(Exception):
            assert TaskTimeout(manual_context, poke_interval, timeout)


    @pytest.mark.usefixtures("schedule_context")
    @pytest.mark.parametrize("timeout", [
        (60*60*24), (60*60*25), (None)
    ])
    def test_schedule_int_D_is_timeout(self, schedule_context, timeout, mocker):
        # assume that task start from datetime(2019, 4, 23, 1, 0, 0)
        # and emulate time now to test if the task should be timeout base on timeout parameter
        handler = TaskTimeout(schedule_context, poke_interval, timeout)
        assert handler.timeout_dt == TimeUtils().datetime(2019, 4, 24, 1, 0, 0)

        ########################################
        #  [Time Changed] 2019/04/23 00:08:00  #
        ########################################
        # all timeout of tests should return false
        patch_now(mocker, TimeUtils().datetime(2019, 4, 23, 8, 0, 0))
        assert handler.is_timeout() == False

        ########################################
        #  [Time Changed] 2019/04/24 00:00:00  #
        ########################################
        # all timeout of tests should return true (since offset=10)
        # the actual timeout time for is_timeout() would be (timeout_dt - 10 seconds)
        patch_now(mocker, TimeUtils().datetime(2019, 4, 24, 1, 0, 0))
        assert handler.is_timeout() == True


    @pytest.mark.usefixtures("schedule_context")
    def test_schedule_crontab_D_is_timeout(self, schedule_context, mocker):
        # assume that task start from datetime(2019, 4, 23, 1, 0, 0)
        # and emulate time now to test if the task should be timeout base on timeout parameter
        timeout = '* 22 * * *'
        handler = TaskTimeout(schedule_context, poke_interval, timeout)
        assert handler.timeout_dt == TimeUtils().datetime(2019, 4, 23, 22, 0, 0)

        ########################################
        #  [Time Changed] 2019/04/23 00:08:00  #
        ########################################
        # all timeout of tests should return false
        patch_now(mocker, TimeUtils().datetime(2019, 4, 23, 8, 0, 0))
        assert handler.is_timeout() == False

        #######################################
        #  [Time Changed] 2019/04/23 21:59:10 #
        #######################################
        # is_timeout() return False
        # next poke: 2019/04/23 22:00:10 > 2019/04/23 22:00:10
        # so is_next_poke_timeout() return True, and set execute_last_poke_after_secs
        patch_now(mocker, TimeUtils().datetime(2019, 4, 23, 21, 59, 10))
        assert poke_interval == 60
        assert handler.is_timeout() == False
        assert handler.last_poke_offset == 30
        assert handler.is_next_poke_timeout() == True
        assert handler.execute_last_poke_after_secs == 20
        # task would be triggered at around 2019/04/23 21:59:10 + 20 seconds

        #######################################
        #  [Time Changed] 2019/04/23 21:59:30 #
        #######################################
        # enumlate that execute_last_poke_after_secs is not set,
        # and the task is triggered at 2019/04/23 21:59:30
        handler.execute_last_poke_after_secs = None
        patch_now(mocker, TimeUtils().datetime(2019, 4, 23, 21, 59, 30))
        assert handler.is_timeout() == False
        assert handler.last_poke_offset == 30
        assert handler.is_next_poke_timeout() == True
        # only 30 secs before timeout which <= last_poke_offset
        # it won't set to perform the last poke
        assert handler.execute_last_poke_after_secs is None

        ########################################
        #  [Time Changed] 2019/04/23 21:59:50  #
        ########################################
        patch_now(mocker, TimeUtils().datetime(2019, 4, 23, 21, 59, 50))
        assert handler.is_timeout() == False
        assert handler.is_next_poke_timeout() == True
        # only 10 secs before timeout which <= last_poke_offset
        # it won't set to perform the last poke
        assert handler.execute_last_poke_after_secs is None

        ########################################
        #  [Time Changed] 2019/04/23 22:00:00  #
        ########################################
        patch_now(mocker, TimeUtils().datetime(2019, 4, 23, 22, 0, 0))
        assert handler.is_timeout() == True
        assert handler.is_next_poke_timeout() == True


    def test_schedule_crontab_M_is_timeout(self, mocker):
        # assume that task start from datetime(2019, 4, 23, 1, 0, 0)
        # and emulate time now to test if the task should be timeout base on timeout parameter
        # '0 22 L * *' works as every 22:00 of the last day of month
        timeout = '0 22 22 * *'
        schedule_M_context = {
            'execution_date': TimeUtils().datetime(2019, 4, 23, 1, 0, 0),
            'next_execution_date': TimeUtils().datetime(2019, 5, 23, 1, 0, 0)
        }
        handler = TaskTimeout(schedule_M_context, poke_interval, timeout)
        assert handler.timeout_dt == TimeUtils().datetime(2019, 6, 22, 22, 0, 0)

        ########################################
        #  [Time Changed] 2019/06/01 00:08:00  #
        ########################################
        # all timeout of tests should return false
        patch_now(mocker, TimeUtils().datetime(2019, 5, 28, 8, 0, 0))
        assert handler.is_timeout() == False
        patch_now(mocker, TimeUtils().datetime(2019, 6, 1, 8, 0, 0))
        assert handler.is_timeout() == False

        #######################################
        #  [Time Changed] 2019/06/22 21:59:10 #
        #######################################
        # is_timeout() return False
        # next poke: 2019/04/23 22:00:10 > 2019/04/23 22:00:10
        # so is_next_poke_timeout() return True, and set execute_last_poke_after_secs
        patch_now(mocker, TimeUtils().datetime(2019, 6, 22, 21, 59, 10))
        assert poke_interval == 60
        assert handler.is_timeout() == False
        assert handler.last_poke_offset == 30
        assert handler.is_next_poke_timeout() == True
        assert handler.execute_last_poke_after_secs == 20
        # task would be triggered at around 2019/04/23 21:59:10 + 20 seconds

        #######################################
        #  [Time Changed] 2019/06/22 21:59:30 #
        #######################################
        # enumlate that execute_last_poke_after_secs is not set,
        # and the task is triggered at 2019/06/22 21:59:30
        handler.execute_last_poke_after_secs = None
        patch_now(mocker, TimeUtils().datetime(2019, 6, 22, 21, 59, 30))
        assert handler.is_timeout() == False
        assert handler.last_poke_offset == 30
        assert handler.is_next_poke_timeout() == True
        # only 30 secs before timeout which <= last_poke_offset
        # it won't set to perform the last poke
        assert handler.execute_last_poke_after_secs is None

        ########################################
        #  [Time Changed] 2019/06/22 21:59:50  #
        ########################################
        patch_now(mocker, TimeUtils().datetime(2019, 6, 22, 21, 59, 50))
        assert handler.is_timeout() == False
        assert handler.is_next_poke_timeout() == True
        # only 10 secs before timeout which <= last_poke_offset
        # it won't set to perform the last poke
        assert handler.execute_last_poke_after_secs is None

        ########################################
        #  [Time Changed] 2019/06/22 22:00:00  #
        ########################################
        patch_now(mocker, TimeUtils().datetime(2019, 6, 22, 22, 0, 0))
        assert handler.is_timeout() == True
        assert handler.is_next_poke_timeout() == True
