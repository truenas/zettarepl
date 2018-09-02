# -*- coding=utf-8 -*-
from mock import Mock

from zettarepl.replication.run import get_snapshot_to_send
from zettarepl.scheduler.cron import CronSchedule


def test__get_snapshot_to_send__works():
    assert get_snapshot_to_send(
        Mock(restrict_schedule=None, naming_schema="%Y-%m-%d_%H-%M"),
        "data",
        ["2018-09-02_17-45", "2018-09-02_17-46", "2018-09-02_17-47"]
    ) == "2018-09-02_17-47"


def test__get_snapshot_to_send__restrict_schedule():
    assert get_snapshot_to_send(
        Mock(restrict_schedule=CronSchedule("*/2", "*", "*", "*", "*"), naming_schema="%Y-%m-%d_%H-%M"),
        "data",
        ["2018-09-02_17-45", "2018-09-02_17-46", "2018-09-02_17-47"]
    ) == "2018-09-02_17-46"
