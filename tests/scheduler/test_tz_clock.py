# -*- coding=utf-8 -*-
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from zettarepl.scheduler.tz_clock import *


def _local(utc: datetime, tz: ZoneInfo) -> datetime:
    return utc.replace(tzinfo=timezone.utc).astimezone(tz)


def test__legit_time_backward():
    tz = ZoneInfo("Europe/Moscow")

    tz_clock = TzClock(tz, datetime(2010, 10, 30, 22, 59, 59))

    expected_local = _local(datetime(2010, 10, 30, 23, 0, 0), tz)
    assert tz_clock.tick(datetime(2010, 10, 30, 23, 0, 0)) == TzClockDateTime(
        expected_local.replace(tzinfo=None),
        expected_local,
        datetime(2010, 10, 30, 23, 0, 0),
        timedelta(hours=1),
    )


def test__nonlegit_time_backward():
    tz = ZoneInfo("Europe/Moscow")

    tz_clock = TzClock(tz, datetime(2010, 8, 30, 22, 59, 59))

    expected_local = _local(datetime(2010, 8, 30, 22, 59, 58), tz)
    assert tz_clock.tick(datetime(2010, 8, 30, 22, 59, 58)) == TzClockDateTime(
        expected_local.replace(tzinfo=None),
        expected_local,
        datetime(2010, 8, 30, 22, 59, 58),
        None,
    )


def test__time_forward():
    tz = ZoneInfo("Europe/Moscow")

    tz_clock = TzClock(tz, datetime(2010, 8, 30, 22, 59, 59))

    expected_local = _local(datetime(2010, 8, 30, 23, 0, 0), tz)
    assert tz_clock.tick(datetime(2010, 8, 30, 23, 0, 0)) == TzClockDateTime(
        expected_local.replace(tzinfo=None),
        expected_local,
        datetime(2010, 8, 30, 23, 0, 0),
        None,
    )
