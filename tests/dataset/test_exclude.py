# -*- coding=utf-8 -*-
import pytest

from zettarepl.dataset.exclude import should_exclude


@pytest.mark.parametrize("dataset,exclude,result", [
    ("data", ["data/.system"], False),
    ("data/.system", ["data/.system"], True),
    ("data/.system/cores", ["data/.system"], True),
    ("data/.system-settings", ["data/.system"], False),
    ("my-data", ["data/.system"], False),
    ("my-data/.system", ["data/.system"], False),
    ("data/.system/cores", ["data/*/cores"], True),
    ("data/.system/cores2", ["data/*/cores"], False),
])
def test__should_exclude(dataset, exclude, result):
    assert should_exclude(dataset, exclude) == result
