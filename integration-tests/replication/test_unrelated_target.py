# -*- coding=utf-8 -*-
import logging
import subprocess
import textwrap

import pytest
import yaml

from zettarepl.snapshot.list import list_snapshots
from zettarepl.snapshot.snapshot import Snapshot
from zettarepl.transport.local import LocalShell
from zettarepl.utils.test import create_dataset, run_replication_test, transports


def guid(snapshot):
    return subprocess.check_output(["zfs", "get", "-H", "-o", "value", "guid", snapshot], encoding="utf-8").strip()


def definition(**kwargs):
    result = yaml.safe_load(textwrap.dedent("""\
        timezone: "UTC"

        replication-tasks:
          src:
            direction: push
            transport:
              type: local
            source-dataset: tank/src
            target-dataset: tank/dst
            recursive: true
            also-include-naming-schema:
              - "%Y-%m-%d_%H-%M"
            auto: false
            retention-policy: none
            retries: 1
    """))
    result["replication-tasks"]["src"].update(**kwargs)
    return result


def create_unrelated_src_and_dst():
    subprocess.call("zfs destroy -r tank/src", shell=True)
    subprocess.call("zfs receive -A tank/dst", shell=True)
    subprocess.call("zfs destroy -r tank/dst", shell=True)

    create_dataset("tank/src")
    subprocess.check_call("zfs snapshot tank/src@2018-10-01_01-00", shell=True)
    subprocess.check_call("zfs snapshot tank/src@2018-10-01_02-00", shell=True)

    create_dataset("tank/dst")
    subprocess.check_call("zfs snapshot tank/dst@2018-10-01_01-00", shell=True)
    subprocess.check_call("zfs snapshot tank/dst@2018-10-01_02-00", shell=True)


@pytest.mark.parametrize("transport", transports())
def test_unrelated_target_with_same_snapshot_names(transport):
    create_unrelated_src_and_dst()

    dst_guid = guid("tank/dst@2018-10-01_02-00")

    error = run_replication_test(definition(transport=transport), success=False).error

    assert (
        "Snapshot tank/dst@2018-10-01_02-00 has a different GUID than source snapshot "
        "tank/src@2018-10-01_02-00" in error
    )
    assert guid("tank/dst@2018-10-01_02-00") == dst_guid


def test_unrelated_target_with_same_snapshot_names_from_scratch():
    create_unrelated_src_and_dst()

    run_replication_test(definition(**{"allow-from-scratch": True}))

    assert guid("tank/dst@2018-10-01_01-00") == guid("tank/src@2018-10-01_01-00")
    assert guid("tank/dst@2018-10-01_02-00") == guid("tank/src@2018-10-01_02-00")


def test_diverged_newest_target_snapshot_from_scratch(caplog):
    subprocess.call("zfs destroy -r tank/src", shell=True)
    subprocess.call("zfs receive -A tank/dst", shell=True)
    subprocess.call("zfs destroy -r tank/dst", shell=True)

    create_dataset("tank/src")
    subprocess.check_call("zfs snapshot tank/src@2018-10-01_01-00", shell=True)

    run_replication_test(definition())

    subprocess.check_call("zfs snapshot tank/src@2018-10-01_02-00", shell=True)
    subprocess.check_call("zfs snapshot tank/dst@2018-10-01_02-00", shell=True)

    caplog.set_level(logging.INFO)
    run_replication_test(definition(**{"allow-from-scratch": True}))

    assert guid("tank/dst@2018-10-01_01-00") == guid("tank/src@2018-10-01_01-00")
    assert guid("tank/dst@2018-10-01_02-00") == guid("tank/src@2018-10-01_02-00")
    assert any(
        "Destroying diverged target dataset snapshots ['2018-10-01_02-00']" in record.getMessage()
        for record in caplog.get_records("call")
    )
    assert any(
        "incremental_base='2018-10-01_01-00'" in record.getMessage()
        for record in caplog.get_records("call")
    )


def test_unrelated_child_target():
    subprocess.call("zfs destroy -r tank/src", shell=True)
    subprocess.call("zfs receive -A tank/dst", shell=True)
    subprocess.call("zfs destroy -r tank/dst", shell=True)

    create_dataset("tank/src")
    create_dataset("tank/src/child")
    subprocess.check_call("zfs snapshot -r tank/src@2018-10-01_01-00", shell=True)

    run_replication_test(definition())

    subprocess.check_call("zfs destroy -r tank/dst/child", shell=True)
    create_dataset("tank/dst/child")
    subprocess.check_call("zfs snapshot tank/dst/child@2018-10-01_01-00", shell=True)

    error = run_replication_test(definition(), success=False).error

    assert (
        "Snapshot tank/dst/child@2018-10-01_01-00 has a different GUID than source snapshot "
        "tank/src/child@2018-10-01_01-00" in error
    )


def test_related_target_replicates_incrementally():
    subprocess.call("zfs destroy -r tank/src", shell=True)
    subprocess.call("zfs receive -A tank/dst", shell=True)
    subprocess.call("zfs destroy -r tank/dst", shell=True)

    create_dataset("tank/src")
    subprocess.check_call("zfs snapshot tank/src@2018-10-01_01-00", shell=True)

    run_replication_test(definition())

    subprocess.check_call("zfs snapshot tank/src@2018-10-01_02-00", shell=True)

    run_replication_test(definition())

    local_shell = LocalShell()
    assert list_snapshots(local_shell, "tank/dst", False) == [
        Snapshot("tank/dst", "2018-10-01_01-00"),
        Snapshot("tank/dst", "2018-10-01_02-00"),
    ]
