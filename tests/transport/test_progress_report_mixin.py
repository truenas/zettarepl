# -*- coding=utf-8 -*-
import pytest

from zettarepl.transport.progress_report_mixin import parse_zfs_progress


@pytest.mark.parametrize("s,result", [
    ('COMMAND\nzfs: sending data/src/src1@2018-10-01_02-00 (96%: 9437184/1048576)\n', (9437184, 1048576)),
    ('COMMAND\nzfs: sending data/src/src1@2018-10-01_02-00 (96%: 1.00M/1.04M)\n', (1000000, 1040000)),
])
def test_parse_zfs_progress(s, result):
    assert parse_zfs_progress(s) == result
