# -*- coding=utf-8 -*-
from unittest.mock import Mock, patch

import pytest

from zettarepl.utils.logging import LongStringsFilter


@pytest.mark.parametrize("length,input,output", [
    ("16", "aaaabbbbccccddddeeee", "aaaabb....ddeeee"),
    ("20", "aaaabbbbccccddddeeee", "aaaabbbbccccddddeeee"),
    ("32", "aaaabbbbccccddddeeee", "aaaabbbbccccddddeeee"),
    (
            "8",
            ("aaaabbbbcccc", 1, ["ddddeeeeffff"], {"gg": "hhhhiiiikkkk"}),
            ("aa....cc", 1, ["dd....ff"], {"gg": "hh....kk"})
    ),
])
def test__long_strings_filter(length, input, output):
    with patch("zettarepl.utils.logging.os.environ.get", Mock(return_value=length)):
        record = Mock(args=input)
        LongStringsFilter().filter(record)
        assert record.args == output
