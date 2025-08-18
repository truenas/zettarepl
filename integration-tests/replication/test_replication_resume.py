# -*- coding=utf-8 -*-
import logging
import os
import subprocess
import textwrap

import libzfs
import pytest
import yaml

from zettarepl.snapshot.list import list_snapshots
from zettarepl.transport.local import LocalShell
from zettarepl.utils.test import create_dataset, run_replication_test, transports


@pytest.mark.parametrize("transport", transports())
@pytest.mark.parametrize("dedup", [False, True])
@pytest.mark.parametrize("encrypted", [True, False])
def test_replication_resume(caplog, transport, dedup, encrypted):
    if dedup and not hasattr(libzfs.SendFlags, "DEDUP"):
        return

    subprocess.call("zfs destroy -r tank/src", shell=True)
    subprocess.call("zfs receive -A tank/dst", shell=True)
    subprocess.call("zfs destroy -r tank/dst", shell=True)

    create_dataset("tank/src", encrypted)
    subprocess.check_call("dd if=/dev/urandom of=/mnt/tank/src/blob bs=1M count=1", shell=True)
    subprocess.check_call("zfs snapshot tank/src@2018-10-01_01-00", shell=True)

    if encrypted:
        subprocess.check_call("(zfs send -p -w tank/src@2018-10-01_01-00 | throttle -b 102400 | zfs recv -s -F tank/dst) & "
                              "sleep 1; killall zfs", shell=True)
    else:
        subprocess.check_call("zfs create tank/dst", shell=True)
        subprocess.check_call("(zfs send tank/src@2018-10-01_01-00 | throttle -b 102400 | zfs recv -s -F tank/dst) & "
                              "sleep 1; killall zfs", shell=True)

    assert "receive_resume_token\t1-" in subprocess.check_output("zfs get -H receive_resume_token tank/dst",
                                                                 shell=True, encoding="utf-8")

    definition = yaml.safe_load(textwrap.dedent("""\
        timezone: "UTC"

        periodic-snapshot-tasks:
          src:
            dataset: tank/src
            recursive: true
            lifetime: PT1H
            naming-schema: "%Y-%m-%d_%H-%M"
            schedule:
              minute: "0"

        replication-tasks:
          src:
            direction: push
            source-dataset: tank/src
            target-dataset: tank/dst
            recursive: true
            periodic-snapshot-tasks:
              - src
            auto: true
            retention-policy: none
    """))
    definition["replication-tasks"]["src"]["transport"] = transport
    definition["replication-tasks"]["src"]["dedup"] = dedup
    if encrypted:
        definition["replication-tasks"]["src"]["properties"] = True

    caplog.set_level(logging.INFO)
    run_replication_test(definition)

    assert any(
        "Resuming replication for destination dataset" in record.message
        for record in caplog.get_records("call")
    )

    local_shell = LocalShell()
    assert len(list_snapshots(local_shell, "tank/dst", False)) == 1

    if not encrypted:
        assert subprocess.check_output("zfs get -H -o value mounted tank/dst", shell=True, encoding="utf-8") == "yes\n"


@pytest.mark.parametrize("canmount", [True, False])
def test_replication_resume__recursive_mount(canmount):
    subprocess.call("zfs destroy -r tank/src", shell=True)
    subprocess.call("zfs receive -A tank/dst", shell=True)
    subprocess.call("zfs destroy -r tank/dst", shell=True)

    create_dataset("tank/src")
    create_dataset("tank/src/child")
    subprocess.check_call("zfs snapshot -r tank/src@2018-10-01_01-00", shell=True)
    subprocess.check_call("zfs send -R tank/src@2018-10-01_01-00 | zfs recv -s -F tank/dst", shell=True)
    if not canmount:
        subprocess.check_call("zfs set canmount=off tank/dst", shell=True)
        subprocess.check_call("zfs set canmount=off tank/dst/child", shell=True)

    subprocess.check_call("dd if=/dev/urandom of=/mnt/tank/src/blob bs=1M count=1", shell=True)
    subprocess.check_call("zfs snapshot tank/src@2018-10-01_02-00", shell=True)

    subprocess.check_call("(zfs send -i tank/src@2018-10-01_01-00 tank/src@2018-10-01_02-00 | throttle -b 102400 | zfs recv -s -F tank/dst) & "
                          "sleep 1; killall zfs", shell=True)

    assert "receive_resume_token\t1-" in subprocess.check_output("zfs get -H receive_resume_token tank/dst",
                                                                 shell=True, encoding="utf-8")

    definition = yaml.safe_load(textwrap.dedent("""\
        timezone: "UTC"

        periodic-snapshot-tasks:
          src:
            dataset: tank/src
            recursive: true
            lifetime: PT1H
            naming-schema: "%Y-%m-%d_%H-%M"
            schedule:
              minute: "0"

        replication-tasks:
          src:
            direction: push
            transport:
              type: local
            source-dataset: tank/src
            target-dataset: tank/dst
            recursive: true
            periodic-snapshot-tasks:
              - src
            auto: true
            retention-policy: none
    """))

    run_replication_test(definition)

    mounted = subprocess.check_output("zfs get -H -o value mounted tank/dst/child", shell=True, encoding="utf-8")
    if canmount:
        assert mounted == "yes\n"
    else:
        assert mounted == "no\n"


@pytest.mark.parametrize("kill_timeout", [1, 15])
@pytest.mark.parametrize("had_recursive_replication_before", [False, True])
def test_replication_resume__replicate(caplog, kill_timeout, had_recursive_replication_before):
    if had_recursive_replication_before and kill_timeout != 1:
        return

    subprocess.call("zfs destroy -r tank/src", shell=True)
    subprocess.call("zfs receive -A tank/dst", shell=True)
    subprocess.call("zfs destroy -r tank/dst", shell=True)

    create_dataset("tank/src")
    create_dataset("tank/src/child")
    subprocess.check_call("dd if=/dev/urandom of=/mnt/tank/src/blob bs=1M count=1", shell=True)
    subprocess.check_call("dd if=/dev/urandom of=/mnt/tank/src/child/blob bs=1M count=1", shell=True)
    subprocess.check_call("zfs snapshot -r tank/src@2018-10-01_01-00", shell=True)

    if had_recursive_replication_before:
        subprocess.check_call("zfs send -R tank/src@2018-10-01_01-00 | zfs recv -s -F tank/dst", shell=True)

        subprocess.check_call("dd if=/dev/urandom of=/mnt/tank/src/blob bs=1M count=1", shell=True)
        subprocess.check_call("dd if=/dev/urandom of=/mnt/tank/src/child/blob bs=1M count=1", shell=True)
        subprocess.check_call("zfs snapshot -r tank/src@2018-10-01_02-00", shell=True)
        subprocess.check_call(
            "(zfs send -p -i tank/src/child@2018-10-01_01-00 tank/src/child@2018-10-01_02-00 | throttle -b 102400 |"
            " zfs recv -s -F tank/dst/child) & "
            f"sleep {kill_timeout}; killall zfs",
            shell=True,
        )
        assert "receive_resume_token\t1-" in subprocess.check_output("zfs get -H receive_resume_token tank/dst/child",
                                                                     shell=True, encoding="utf-8")
    else:
        subprocess.check_call(
            "(zfs send -R tank/src@2018-10-01_01-00 | throttle -b 102400 | zfs recv -s -F tank/dst) & "
            f"sleep {kill_timeout}; killall zfs",
            shell=True,
        )
        assert "receive_resume_token\t1-" in subprocess.check_output("zfs get -H receive_resume_token tank/dst",
                                                                     shell=True, encoding="utf-8")

    definition = yaml.safe_load(textwrap.dedent("""\
        timezone: "UTC"

        periodic-snapshot-tasks:
          src:
            dataset: tank/src
            recursive: true
            lifetime: PT1H
            naming-schema: "%Y-%m-%d_%H-%M"
            schedule:
              minute: "0"

        replication-tasks:
          src:
            direction: push
            transport:
              type: local
            source-dataset: tank/src
            target-dataset: tank/dst
            recursive: true
            replicate: true
            periodic-snapshot-tasks:
              - src
            auto: true
            retention-policy: none
    """))

    caplog.set_level(logging.INFO)
    run_replication_test(definition)

    assert any(
        "Discarding receive_resume_token" in record.message
        for record in caplog.get_records("call")
    )

    local_shell = LocalShell()
    if had_recursive_replication_before:
        assert len(list_snapshots(local_shell, "tank/dst", False)) == 2
        assert len(list_snapshots(local_shell, "tank/dst/child", False)) == 2
    else:
        assert len(list_snapshots(local_shell, "tank/dst", False)) == 1
        assert len(list_snapshots(local_shell, "tank/dst/child", False)) == 1
