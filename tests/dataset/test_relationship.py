# -*- coding=utf-8 -*-
import pytest

from zettarepl.dataset.relationship import is_child


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
