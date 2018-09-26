# -*- coding=utf-8 -*-
import pytest
from unittest.mock import Mock, patch

from zettarepl.utils.test import mock_name

from zettarepl.replication.task.snapshot_owner import PendingReplicationTaskSnapshotOwner


@pytest.mark.parametrize("replication_task,src_snapshots,dst_snapshots,dataset,snapshot_name,should_retain", [
    # Everything is ok
    (
        Mock(source_dataset="data/work",
             target_dataset="repl/work",
             recursive=False),
        {
            "data/work": ["2018-09-26_11-47", "2018-09-26_11-48", "2018-09-26_11-49"],
        },
        {
            "repl/work": ["2018-09-26_11-47", "2018-09-26_11-48", "2018-09-26_11-49"],
        },
        "data/work",
        "2018-09-26_11-49",
        False,
    ),
    # Replication probably failed
    (
        Mock(source_dataset="data/work",
             target_dataset="repl/work",
             recursive=False),
        {
            "data/work": ["2018-09-26_11-47", "2018-09-26_11-48", "2018-09-26_11-49"],
        },
        {
            "repl/work": ["2018-09-26_11-47"],
        },
        "data/work",
        "2018-09-26_11-49",
        True,
    ),
    # There was no data/work/ix at 2018-09-26_11-47
    (
        Mock(source_dataset="data/work",
             target_dataset="repl/work",
             recursive=True,
             exclude=[]),
        {
            "data/work": ["2018-09-26_11-47", "2018-09-26_11-48", "2018-09-26_11-49"],
            "data/work/ix": ["2018-09-26_11-48", "2018-09-26_11-49"],
        },
        {
            "repl/work": ["2018-09-26_11-47", "2018-09-26_11-48", "2018-09-26_11-49"],
            "repl/work/ix": ["2018-09-26_11-48", "2018-09-26_11-49"],
        },
        "data/work/ix",
        "2018-09-26_11-47",
        False,
    ),
    # Replication of repl/work/ix@2018-09-26_11-49 failed
    (
        Mock(source_dataset="data/work",
             target_dataset="repl/work",
             recursive=True,
             exclude=[]),
        {
            "data/work": ["2018-09-26_11-47", "2018-09-26_11-48", "2018-09-26_11-49"],
            "data/work/ix": ["2018-09-26_11-48", "2018-09-26_11-49"],
        },
        {
            "repl/work": ["2018-09-26_11-47", "2018-09-26_11-48", "2018-09-26_11-49"],
            "repl/work/ix": ["2018-09-26_11-48"],
        },
        "data/work/ix",
        "2018-09-26_11-49",
        True,
    ),
    # Same but non-recursive and we don't care
    (
        Mock(source_dataset="data/work",
             target_dataset="repl/work",
             recursive=False),
        {
            "data/work": ["2018-09-26_11-47", "2018-09-26_11-48", "2018-09-26_11-49"],
            "data/work/ix": ["2018-09-26_11-48", "2018-09-26_11-49"],
        },
        {
            "repl/work": ["2018-09-26_11-47", "2018-09-26_11-48", "2018-09-26_11-49"],
            "repl/work/ix": ["2018-09-26_11-48"],
        },
        "data/work/ix",
        "2018-09-26_11-49",
        False,
    ),
])
def test__pending_replication_task_snapshot_owner(replication_task, src_snapshots, dst_snapshots,
                                                  dataset, snapshot_name, should_retain):
    replication_task.owns.return_value = True

    parsed_snapshot_name = Mock()
    mock_name(parsed_snapshot_name, snapshot_name)

    with patch("zettarepl.replication.task.snapshot_owner.replication_task_naming_schemas"):
        snapshot_owner = PendingReplicationTaskSnapshotOwner(replication_task, src_snapshots, dst_snapshots)
        snapshot_owner.owns = lambda *args, **kwargs: True

    assert snapshot_owner.should_retain(dataset, parsed_snapshot_name) == should_retain
