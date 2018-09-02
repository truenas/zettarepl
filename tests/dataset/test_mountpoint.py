# -*- coding=utf-8 -*-
import textwrap

from mock import Mock

from zettarepl.dataset.mountpoint import *


def test__dataset_mountpoints():
    shell = Mock()
    shell.exec.return_value = textwrap.dedent("""\
        data	/mnt/data
        data/my dataset	/mnt/data/my dataset
    """)

    assert dataset_mountpoints(shell, "data", True, [], Mock())["data/my dataset"] == "/mnt/data/my dataset"


def test__dataset_mountpoints_exclude():
    shell = Mock()
    shell.exec.return_value = textwrap.dedent("""\
        data	/mnt/data
        data/my dataset	/mnt/data/my dataset
    """)

    assert "data/my dataset" not in dataset_mountpoints(shell, "data", True, ["data/my dataset"], Mock())


def test__dataset_mountpoints__legacy_mountpoint():
    shell = Mock()
    shell.exec.return_value = textwrap.dedent("""\
        data	/mnt/data
        data/test	-
    """)

    assert dataset_mountpoints(shell, "data", True, [], Mock())["data/test"] == "/mnt/data/test"


def test__dataset_mountpoints__mountpoint_not_specified():
    shell = Mock()
    shell.exec.return_value = textwrap.dedent("""\
        data	/mnt/data
        data/.system/cores	legacy
    """)

    mtab = Mock()
    mtab.get.return_value = "/var/db/system/cores"

    assert dataset_mountpoints(shell, "data", True, [], mtab)["data/.system/cores"] == "/var/db/system/cores"
    mtab.get.assert_called_once_with("data/.system/cores")
