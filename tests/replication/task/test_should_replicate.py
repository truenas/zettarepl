# -*- coding=utf-8 -*-
import pytest
from unittest.mock import Mock

from zettarepl.replication.task.should_replicate import replication_task_replicates_target_dataset


@pytest.mark.parametrize("replication_task,dataset,result", [
    (
        Mock(source_dataset="data/work",
             target_dataset="repl/work",
             recursive=True,
             exclude=["data/work/garbage"]),
        "repl/work/ix",
        True,
    ),
    (
        Mock(source_dataset="data/work",
             target_dataset="repl/work",
             recursive=True,
             exclude=["data/work/garbage"]),
        "repl/work/garbage",
        False,
    )
])
def test__replication_task_replicates_target_dataset(replication_task, dataset, result):
    assert replication_task_replicates_target_dataset(replication_task, dataset) == result
