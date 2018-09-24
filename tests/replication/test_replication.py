# -*- coding=utf-8 -*-
from datetime import datetime

from unittest.mock import ANY, call, Mock, patch

from zettarepl.replication.replication import Replication
from zettarepl.replication.task.task import ReplicationTask
from zettarepl.snapshot.snapshot import Snapshot


def test__run_periodic_snapshot_tasks__alphabetical():
    with patch("zettarepl.replication.replication.create_snapshot") as create_snapshot:
        with patch("zettarepl.replication.replication.get_empty_snapshots_for_deletion", Mock(return_value=[])):
            replication = Replication(Mock(), Mock())
            replication._run_periodic_snapshot_tasks(
                datetime(2018, 9, 1, 15, 11),
                [
                    Mock(dataset="data", recursive=False, naming_schema="snap-%Y-%m-%d_%H-%M-2d"),
                    Mock(dataset="data", recursive=False, naming_schema="snap-%Y-%m-%d_%H-%M-1w"),
                ]
            )

            assert create_snapshot.call_count == 2
            create_snapshot.assert_has_calls([
                call(ANY, Snapshot("data", "snap-2018-09-01_15-11-1w"), False, ANY),
                call(ANY, Snapshot("data", "snap-2018-09-01_15-11-2d"), False, ANY),
            ])


def test__run_periodic_snapshot_tasks__recursive():
    with patch("zettarepl.replication.replication.create_snapshot") as create_snapshot:
        with patch("zettarepl.replication.replication.get_empty_snapshots_for_deletion", Mock(return_value=[])):
            replication = Replication(Mock(), Mock())
            replication._run_periodic_snapshot_tasks(
                datetime(2018, 9, 1, 15, 11),
                [
                    Mock(dataset="data", recursive=False, naming_schema="snap-%Y-%m-%d_%H-%M"),
                    Mock(dataset="data", recursive=True, naming_schema="snap-%Y-%m-%d_%H-%M"),
                ]
            )

            create_snapshot.assert_called_once_with(ANY, Snapshot("data", "snap-2018-09-01_15-11"), True, ANY)


def test__replication_tasks_for_periodic_snapshot_tasks():
    replication = Replication(Mock(), Mock())

    pst1 = Mock()
    pst2 = Mock()
    pst3 = Mock()

    rt1 = Mock(spec=ReplicationTask)
    rt1.periodic_snapshot_tasks = [pst1, pst2]

    rt2 = Mock(spec=ReplicationTask)
    rt2.periodic_snapshot_tasks = []

    assert replication._replication_tasks_for_periodic_snapshot_tasks([rt1, rt2], [pst1, pst3]) == [rt1]
