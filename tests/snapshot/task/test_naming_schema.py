# -*- coding=utf-8 -*-
import pytest

from zettarepl.snapshot.task.naming_schema import *


def test__validate_snapshot_naming_schema__ok():
    validate_snapshot_naming_schema("snap_%Y%m%d_%H%M")


def test__validate_snapshot_naming_schema__validates_full_year_presence():
    with pytest.raises(ValueError) as e:
        validate_snapshot_naming_schema("snap_%y%m%d_%H%M")

    assert e.value.args[0] == "%Y must be present in snapshot naming schema"


def test__validate_snapshot_naming_schema__validates_full_year_presence__cant_fool_validator():
    with pytest.raises(ValueError) as e:
        validate_snapshot_naming_schema("snap_%y%m%d_%H%M%%Y")

    assert e.value.args[0] == "%Y must be present in snapshot naming schema"
