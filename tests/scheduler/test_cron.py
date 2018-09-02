# -*- coding=utf-8 -*-
from datetime import datetime

from zettarepl.scheduler.cron import CronSchedule


def test__works():
    schedule = CronSchedule(0, "*", "*", "*", "*")
    assert schedule.should_run(datetime(2018, 8, 31, 16, 0, 5, 54412))
