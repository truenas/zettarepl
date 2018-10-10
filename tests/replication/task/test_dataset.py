# -*- coding=utf-8 -*-
from unittest.mock import Mock

import pytest

from zettarepl.replication.task.dataset import get_source_dataset_base, get_source_dataset, get_target_dataset


@pytest.mark.parametrize("source_datasets,base", [
    (["data/work", "data/windows"], "data"),
    (["data/work/a", "data/work/b"], "data/work"),
    (["data", "ix-data"], "")
])
def test__get_source_dataset_base(source_datasets, base):
    assert get_source_dataset_base(Mock(source_datasets=source_datasets)) == base


@pytest.mark.parametrize("source_datasets,target_dataset,dst_dataset,result", [
    (["data/work", "data/windows"], "backup", "backup/windows", "data/windows"),
    (["data/work"], "backup", "backup/excel", "data/work/excel"),
])
def test__get_source_dataset(source_datasets, target_dataset, dst_dataset, result):
    assert get_source_dataset(Mock(source_datasets=source_datasets, target_dataset=target_dataset),
                              dst_dataset) == result


@pytest.mark.parametrize("source_datasets,target_dataset,src_dataset,result", [
    (["data/work", "data/windows"], "backup", "data/windows/x", "backup/windows/x"),
])
def test__get_target_dataset(source_datasets, target_dataset, src_dataset, result):
    assert get_target_dataset(Mock(source_datasets=source_datasets, target_dataset=target_dataset),
                              src_dataset) == result
