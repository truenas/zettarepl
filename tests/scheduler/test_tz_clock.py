# -*- coding=utf-8 -*-
from datetime import datetime, timedelta

from pytz import timezone

from zettarepl.scheduler.tz_clock import *


def test__legit_time_backward():
    tz = timezone("Europe/Moscow")

    tz_clock = TzClock(tz, datetime(2010, 10, 30, 22, 59, 59))

    assert tz_clock.tick(datetime(2010, 10, 30, 23, 0, 0)) == TzClockDateTime(
        tz.localize(datetime(2010, 10, 31, 2, 0, 0)).replace(tzinfo=None),
        tz.localize(datetime(2010, 10, 31, 2, 0, 0)),
        datetime(2010, 10, 30, 23, 0, 0),
        timedelta(hours=1),
    )


def test__nonlegit_time_backward():
    tz = timezone("Europe/Moscow")

    tz_clock = TzClock(tz, datetime(2010, 8, 30, 22, 59, 59))

    assert tz_clock.tick(datetime(2010, 8, 30, 22, 59, 58)) == TzClockDateTime(
        tz.localize(datetime(2010, 8, 31, 2, 59, 58)).replace(tzinfo=None),
        tz.localize(datetime(2010, 8, 31, 2, 59, 58)),
        datetime(2010, 8, 30, 22, 59, 58),
        None,
    )


def test__time_forward():
    tz = timezone("Europe/Moscow")

    tz_clock = TzClock(tz, datetime(2010, 8, 30, 22, 59, 59))

    assert tz_clock.tick(datetime(2010, 8, 30, 23, 0, 0)) == TzClockDateTime(
        tz.localize(datetime(2010, 8, 31, 3, 0, 0)).replace(tzinfo=None),
        tz.localize(datetime(2010, 8, 31, 3, 0, 0)),
        datetime(2010, 8, 30, 23, 0, 0),
        None,
    )
