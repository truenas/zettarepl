# -*- coding=utf-8 -*-
import pytest
from unittest.mock import Mock, patch

from zettarepl.snapshot.empty import get_empty_snapshots_for_deletion, get_task_snapshots
from zettarepl.snapshot.snapshot import Snapshot


@pytest.mark.parametrize("datasets,tasks_with_snapshot_names,result", [
    (
            ["data/src", "data/src/work"],
            [
                (Mock(dataset="data/src", recursive=True, exclude=[], allow_empty=True), "snap-1"),
                (Mock(dataset="data/src/work", recursive=False, exclude=[], allow_empty=False), "snap-1"),
            ],
            []
    ),
    (
            ["data/src", "data/src/garbage", "data/src/work"],
            [
                (Mock(dataset="data/src", recursive=True, exclude=[], allow_empty=False), "snap-1"),
                (Mock(dataset="data/src", recursive=True, exclude=["data/src/garbage"], allow_empty=True), "snap-1"),
            ],
            [Snapshot("data/src/garbage", "snap-1")]
    ),
])
def test__get_empty_snapshots_for_deletion__1(datasets, tasks_with_snapshot_names, result):
    with patch("zettarepl.snapshot.empty.list_datasets", Mock(return_value=datasets)):
        with patch("zettarepl.snapshot.empty.is_empty_snapshot", Mock(return_value=True)):
            assert get_empty_snapshots_for_deletion(Mock(), tasks_with_snapshot_names) == result


@pytest.mark.parametrize("all_datasets,task,task_datasets", [
    (
            ["data/src", "data/src/work", "data/dst"],
            Mock(dataset="data/src", recursive=True, exclude=[]),
            ["data/src", "data/src/work"],
    ),
    (
            ["data/src", "data/src/garbage", "data/src/work", "data/dst"],
            Mock(dataset="data/src", recursive=True, exclude=["data/src/garbage"]),
            ["data/src", "data/src/work"],
    ),
    (
            ["data/src", "data/src/work", "data2"],
            Mock(dataset="data/src", recursive=True, exclude=[]),
            ["data/src", "data/src/work"],
    ),
    (
            ["data/src", "data/src/work", "data2"],
            Mock(dataset="data/src", recursive=False, exclude=[]),
            ["data/src"],
    ),
])
def test__get_task_snapshots(all_datasets, task, task_datasets):
    assert [snapshot.dataset for snapshot in get_task_snapshots(all_datasets, task, "")] == task_datasets
