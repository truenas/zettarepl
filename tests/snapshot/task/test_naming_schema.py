# -*- coding=utf-8 -*-
import pytest

from zettarepl.snapshot.name import *


@pytest.mark.parametrize("naming_schema",[
    "snap_%Y%m%d_%H%M",
    "snap_%s",
])
def test__validate_snapshot_naming_schema(naming_schema):
    validate_snapshot_naming_schema(naming_schema)


@pytest.mark.parametrize("naming_schema,error",[
    ("snap_%y%m%d_%H%M", "%Y must be present in snapshot naming schema"),
    ("snap_%y%m%d_%H%M%%Y", "% is not an allowed character in ZFS snapshot name"),
    ("snap_%Y%m%d_%H%M$", "$ is not an allowed character in ZFS snapshot name"),
    ("snap_%Y%m%d_%H%M$&", "$& are not allowed characters in ZFS snapshot name"),
    ("snap_%s%z", "No other placeholder can be used with %s in naming schema"),
    ("snap_%Y%m%d%H%M%M", "Invalid naming schema: redefinition of group name 'M' as group 6; was group 5"),
    ("snap_%s%s", "Invalid naming schema: redefinition of group name 's' as group 2; was group 1"),
])
def test__validate_snapshot_naming_schema__error(naming_schema, error):
    with pytest.raises(ValueError) as e:
        validate_snapshot_naming_schema(naming_schema)

    assert e.value.args[0] == error
