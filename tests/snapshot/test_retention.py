# -*- coding=utf-8 -*-
from datetime import datetime, timedelta

from mock import Mock

from zettarepl.snapshot import Snapshot
from zettarepl.snapshot.retention import *


def test__calculate_periodic_snapshot_tasks_retention__1():
    result = calculate_periodic_snapshot_tasks_retention(
        datetime(2018, 8, 31, 23, 0),
        [
            Mock(dataset="data", lifetime=timedelta(days=10), naming_schema="snap-%Y-%m-%d_%H-%M-%S",
                 schedule=Mock(should_run=Mock(return_value=True))),
            Mock(dataset="data", lifetime=timedelta(days=20), naming_schema="snap-%Y-%m-%d_%H-%M-%S",
                 schedule=Mock(should_run=Mock(side_effect=lambda d: d.minute % 2 == 0))),
        ],
        [
            Snapshot("data", "snap-2018-08-21_22-58-00"),
            Snapshot("data", "snap-2018-08-21_22-59-00"),
            Snapshot("data", "snap-2018-08-21_23-00-00"),
            Snapshot("data", "snap-2018-08-21_23-01-00"),
            Snapshot("data", "snap-2018-08-21_23-02-00"),
        ]
    )

    assert sorted(result) == [Snapshot("data", "snap-2018-08-21_22-59-00")]
