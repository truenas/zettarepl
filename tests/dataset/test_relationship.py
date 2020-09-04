# -*- coding=utf-8 -*-
import pytest

from zettarepl.dataset.relationship import is_child, is_immediate_child


@pytest.mark.parametrize("child,parent,result", [
    ("data", "data/.system", False),
    ("data/.system", "data/.system", True),
    ("data/.system/cores", "data/.system", True),
    ("data/.system-settings", "data/.system", False),
    ("my-data", "data/.system", False),
    ("my-data/.system", "data/.system", False),
])
def test__is_child(child, parent, result):
    assert is_child(child, parent) == result


@pytest.mark.parametrize("child,parent,result", [
    ("data/.system", "data/.system", False),
    ("data/.system/cores", "data/.system", True),
    ("data/.system/cores/linux", "data/.system", False),
    ("my-data/.system/cores/linux", "data/.system", False),
])
def test__is_immediate_child(child, parent, result):
    assert is_immediate_child(child, parent) == result
