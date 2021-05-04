# -*- coding=utf-8 -*-
from datetime import datetime

from zettarepl.replication.task.retention_policy import TargetSnapshotRetentionPolicy
from zettarepl.snapshot.name import parse_snapshots_names


def test__custom_snapshot_retention_policy():
    retention_policy = TargetSnapshotRetentionPolicy.from_data({
        "retention-policy": "custom",
        "lifetime": "P1D",
        "lifetimes": {
            "daily": {
                "schedule": {"hour": "0"},
                "lifetime": "P14D",
            },
            "weekly": {
                "schedule": {"hour": "0", "day-of-week": "1"},
                "lifetime": "P30D",
            },
        }
    })

    now = datetime(2021, 4, 21, 13, 00)
    snapshots = parse_snapshots_names([
        # Stays because taken less than a day ago
        "2021-04-20-19-00",
        # Goes because taken more than a day ago
        "2021-04-20-01-00",
        # Stays because is taken daily and should live for 14 days
        "2021-04-20-00-00",
        # Goes because was taken more than 14 days ago
        "2021-04-06-00-00",
        # Stays because was taken on monday
        "2021-04-05-00-00",
    ], "%Y-%m-%d-%H-%M")
    to_remove = [1, 3]

    assert retention_policy.calculate_delete_snapshots(now, None, snapshots) == [snapshots[i].name for i in to_remove]
