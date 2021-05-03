# -*- coding=utf-8 -*-
from datetime import datetime, timedelta
import pytest
from unittest.mock import Mock

from zettarepl.snapshot.snapshot import Snapshot
from zettarepl.snapshot.task.snapshot_owner import PeriodicSnapshotTaskSnapshotOwner
from zettarepl.retention.calculate import calculate_snapshots_to_remove, calculate_dataset_snapshots_to_remove


def test__calculate_snapshots_to_remove():
    assert calculate_snapshots_to_remove(
        [
            PeriodicSnapshotTaskSnapshotOwner(
                datetime(2019, 5, 30, 21, 52),
                Mock(dataset="dst/work",
                     recursive=False,
                     exclude=[],
                     lifetime=timedelta(days=14),
                     naming_schema="auto-%Y-%m-%d_%H-%M")
            ),
            PeriodicSnapshotTaskSnapshotOwner(
                datetime(2019, 5, 30, 21, 52),
                Mock(dataset="dst/work",
                     recursive=False,
                     exclude=[],
                     lifetime=timedelta(hours=1),
                     naming_schema="snap%d%m%Y%H%M")
            ),
        ],
        [Snapshot("dst/work", "snap300520191856"), Snapshot("dst/work", "snap300520191857")]
    ) == [Snapshot("dst/work", "snap300520191856")]


@pytest.mark.parametrize("owners,dataset,snapshots,result", [
    (
        [
            Mock(
                get_naming_schemas=Mock(return_value=["snap-%Y-%m-%d_%H-%M-%S"]),
                owns_dataset=Mock(return_value=True),
                owns_snapshot=Mock(return_value=True),
                should_retain=Mock(side_effect=lambda dataset, parsed_snapshot_name:
                    parsed_snapshot_name.datetime >= datetime(2018, 8, 21, 23, 0))
            ),
            Mock(
                get_naming_schemas=Mock(return_value=["snap-%Y-%m-%d_%H-%M-%S"]),
                owns_dataset=Mock(return_value=True),
                owns_snapshot=Mock(side_effect=lambda dataset, parsed_snapshot_name:
                    parsed_snapshot_name.datetime.minute % 2 == 0),
                should_retain=Mock(side_effect=lambda dataset, parsed_snapshot_name:
                    parsed_snapshot_name.datetime >= datetime(2018, 8, 11, 23, 0))
            ),
        ],
        "data",
        [
            "snap-2018-08-21_22-58-00",
            "snap-2018-08-21_22-59-00",
            "snap-2018-08-21_23-00-00",
            "snap-2018-08-21_23-01-00",
            "snap-2018-08-21_23-02-00",
        ],
        [
            "snap-2018-08-21_22-59-00"
        ]
    )
])
def test__calculate_dataset_snapshots_to_remove(owners, dataset, snapshots, result):
    assert calculate_dataset_snapshots_to_remove(owners, dataset, snapshots) == result
