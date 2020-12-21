# -*- coding=utf-8 -*-
import pytest
from unittest.mock import Mock

from zettarepl.replication.task.task import ReplicationTask


@pytest.mark.parametrize("pst,source_dataset,exclude,raises", [
    (
        [("data", ["data/temp"])],
        ["data/games"],
        [],
        False,
    ),
    (
        [("data", ["data/temp"])],
        ["data"],
        [],
        True,
    ),
    (
        [("data", ["data/temp"])],
        ["data"],
        ["data/temp"],
        False,
    ),
])
def test__validate_exclude(pst, source_dataset, exclude, raises):
    args = (
        {
            "source-dataset": source_dataset,
            "exclude": exclude,
        },
        [
            Mock(dataset=dataset, exclude=exclude)
            for dataset, exclude in pst
        ],
    )

    if raises:
        with pytest.raises(ValueError):
            ReplicationTask._validate_exclude(*args)
    else:
        ReplicationTask._validate_exclude(*args)
