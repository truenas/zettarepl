# -*- coding=utf-8 -*-
from datetime import datetime, time

import pytest

from zettarepl.scheduler.cron import CronSchedule


@pytest.mark.parametrize("schedule,datetime,result", [
    (CronSchedule(0, "*", "*", "*", "*", time(0, 0), time(23, 59)), datetime(2018, 8, 31, 16, 0, 5, 54412), True),
    (CronSchedule(0, "*", "*", "*", "*", time(9, 0), time(15, 00)), datetime(2018, 8, 31, 16, 0, 5, 54412), False),
    (CronSchedule(0, "*", "*", "*", "*", time(15, 0), time(9, 00)), datetime(2018, 8, 31, 16, 0, 5, 54412), True),
    (CronSchedule(0, "*", "*", "*", "*", time(15, 0), time(9, 00)), datetime(2018, 8, 31, 8, 0, 5, 54412), True),
    (CronSchedule(0, "*", "*", "*", "*", time(15, 0), time(9, 00)), datetime(2018, 8, 31, 12, 0, 5, 54412), False),
])
def test__cron(schedule, datetime, result):
    assert schedule.should_run(datetime) == result
