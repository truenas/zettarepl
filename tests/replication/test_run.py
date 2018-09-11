# -*- coding=utf-8 -*-
from unittest.mock import Mock

from zettarepl.replication.run import get_snapshots_to_send, get_target_dataset
from zettarepl.scheduler.cron import CronSchedule


def test__get_snapshot_to_send__works():
    assert get_snapshots_to_send(
        ["2018-09-02_17-45", "2018-09-02_17-46", "2018-09-02_17-47"],
        ["2018-09-02_17-45"],
        Mock(periodic_snapshot_tasks=[Mock(naming_schema="%Y-%m-%d_%H-%M")],
             also_include_naming_schema=[],
             restrict_schedule=None,
             only_matching_schedule=False),
    ) == ("2018-09-02_17-45", ["2018-09-02_17-46", "2018-09-02_17-47"])


def test__get_snapshot_to_send__restrict_schedule():
    assert get_snapshots_to_send(
        ["2018-09-02_17-45", "2018-09-02_17-46", "2018-09-02_17-47"],
        ["2018-09-02_17-45"],
        Mock(periodic_snapshot_tasks=[Mock(naming_schema="%Y-%m-%d_%H-%M")],
             also_include_naming_schema=[],
             restrict_schedule=CronSchedule("*/2", "*", "*", "*", "*"),
             only_matching_schedule=False),
    ) == ("2018-09-02_17-45", ["2018-09-02_17-46"])


def test__get_snapshot_to_send__multiple_tasks():
    assert get_snapshots_to_send(
        ["1w-2018-09-02_00-00", "2d-2018-09-02_00-00", "2d-2018-09-02_12-00",
         "1w-2018-09-03_00-00", "2d-2018-09-03_12-00"],
        ["1w-2018-09-02_00-00", "2d-2018-09-02_00-00"],
        Mock(periodic_snapshot_tasks=[Mock(naming_schema="1w-%Y-%m-%d_%H-%M"),
                                      Mock(naming_schema="2d-%Y-%m-%d_%H-%M")],
             also_include_naming_schema=[],
             restrict_schedule=None,
             only_matching_schedule=False),
    ) == ("2d-2018-09-02_00-00", ["2d-2018-09-02_12-00", "1w-2018-09-03_00-00", "2d-2018-09-03_12-00"])


def test__get_target_dataset__1():
    assert get_target_dataset(
        Mock(source_dataset="data/src", target_dataset="data/dst"),
        "data/src"
    ) == "data/dst"


def test__get_target_dataset__2():
    assert get_target_dataset(
        Mock(source_dataset="data/src", target_dataset="data/dst"),
        "data/src/a/b"
    ) == "data/dst/a/b"
