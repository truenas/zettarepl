# -*- coding=utf-8 -*-
import pytest

from zettarepl.utils.re import re_search_to


@pytest.mark.parametrize("regex,s,result,out", [
    ("Date is: (?P<day>[0-9]+)/([0-9]+)", "Hi! Date is: 08/01", True, {
        0: "Date is: 08/01",
        1: "08",
        2: "01",
        "day": "08",
    }),
    ("Date is: (?P<day>[0-9]+)/([0-9]+)", "Hi! Date Is: 08/01", False, {}),
])
def test__re_search_to(regex, s, result, out):
    m = {}
    assert re_search_to(m, regex, s) == result
    assert m == out
