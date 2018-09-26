# -*- coding=utf-8 -*-
import pytest

from zettarepl.snapshot.list import simplify_snapshot_list_queries


@pytest.mark.parametrize("queries,simple", [
    (
        [("data/work", False), ("data", True)],
        [("data", True)]
    ),
    (
        [("data/work", True), ("data", False)],
        [("data", False), ("data/work", True)]
    ),
    (
        [("data/work", True), ("data", False)],
        [("data", False), ("data/work", True)]
    ),
    (
        [("data/work", True), ("data", False), ("data/home", True)],
        [("data", False), ("data/home", True), ("data/work", True)]
    ),
    (
        [("data/work", False), ("data", False), ("data/work", True)],
        [("data", False), ("data/work", True)]
    ),
    (
        [("data/work", False), ("data", False), ("data/work/home", True)],
        [("data", False), ("data/work", False), ("data/work/home", True)]
    ),
])
def test__simplify_snapshot_list_queries(queries, simple):
    assert simplify_snapshot_list_queries(queries) == simple
