# -*- coding=utf-8 -*-
from datetime import datetime
import pytest
from unittest.mock import Mock

from zettarepl.retention.calculate import calculate_dataset_snapshots_to_remove


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
                owns_snapshot=Mock(side_effect=lambda parsed_snapshot_name:
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
