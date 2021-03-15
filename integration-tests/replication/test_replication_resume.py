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

    subprocess.call("zfs destroy -r data/src", shell=True)
    subprocess.call("zfs receive -A data/dst", shell=True)
    subprocess.call("zfs destroy -r data/dst", shell=True)

    create_dataset("data/src", encrypted)
    subprocess.check_call("dd if=/dev/urandom of=/mnt/data/src/blob bs=1M count=1", shell=True)
    subprocess.check_call("zfs snapshot data/src@2018-10-01_01-00", shell=True)

    if encrypted:
        subprocess.check_call("(zfs send -p -w data/src@2018-10-01_01-00 | throttle -b 102400 | zfs recv -s -F data/dst) & "
                              "sleep 1; killall zfs", shell=True)
    else:
        subprocess.check_call("zfs create data/dst", shell=True)
        subprocess.check_call("(zfs send data/src@2018-10-01_01-00 | throttle -b 102400 | zfs recv -s -F data/dst) & "
                              "sleep 1; killall zfs", shell=True)

    assert "receive_resume_token\t1-" in subprocess.check_output("zfs get -H receive_resume_token data/dst",
                                                                 shell=True, encoding="utf-8")

    definition = yaml.safe_load(textwrap.dedent("""\
        timezone: "UTC"

        periodic-snapshot-tasks:
          src:
            dataset: data/src
            recursive: true
            lifetime: PT1H
            naming-schema: "%Y-%m-%d_%H-%M"
            schedule:
              minute: "0"

        replication-tasks:
          src:
            direction: push
            source-dataset: data/src
            target-dataset: data/dst
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
    assert len(list_snapshots(local_shell, "data/dst", False)) == 1

    if not encrypted:
        assert subprocess.check_output("zfs get -H -o value mounted data/dst", shell=True, encoding="utf-8") == "yes\n"


@pytest.mark.parametrize("canmount", [True, False])
def test_replication_resume__recursive_mount(canmount):
    subprocess.call("zfs destroy -r data/src", shell=True)
    subprocess.call("zfs receive -A data/dst", shell=True)
    subprocess.call("zfs destroy -r data/dst", shell=True)

    create_dataset("data/src")
    create_dataset("data/src/child")
    subprocess.check_call("zfs snapshot -r data/src@2018-10-01_01-00", shell=True)
    subprocess.check_call("zfs send -R data/src@2018-10-01_01-00 | zfs recv -s -F data/dst", shell=True)
    if not canmount:
        subprocess.check_call("zfs set canmount=off data/dst", shell=True)
        subprocess.check_call("zfs set canmount=off data/dst/child", shell=True)

    subprocess.check_call("dd if=/dev/urandom of=/mnt/data/src/blob bs=1M count=1", shell=True)
    subprocess.check_call("zfs snapshot data/src@2018-10-01_02-00", shell=True)

    subprocess.check_call("(zfs send -i data/src@2018-10-01_01-00 data/src@2018-10-01_02-00 | throttle -b 102400 | zfs recv -s -F data/dst) & "
                          "sleep 1; killall zfs", shell=True)

    assert "receive_resume_token\t1-" in subprocess.check_output("zfs get -H receive_resume_token data/dst",
                                                                 shell=True, encoding="utf-8")

    definition = yaml.safe_load(textwrap.dedent("""\
        timezone: "UTC"

        periodic-snapshot-tasks:
          src:
            dataset: data/src
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
            source-dataset: data/src
            target-dataset: data/dst
            recursive: true
            periodic-snapshot-tasks:
              - src
            auto: true
            retention-policy: none
    """))

    run_replication_test(definition)

    mounted = subprocess.check_output("zfs get -H -o value mounted data/dst/child", shell=True, encoding="utf-8")
    if canmount:
        assert mounted == "yes\n"
    else:
        assert mounted == "no\n"


@pytest.mark.parametrize("kill_timeout", [1, 15])
def test_replication_resume__replicate(caplog, kill_timeout):
    subprocess.call("zfs destroy -r data/src", shell=True)
    subprocess.call("zfs receive -A data/dst", shell=True)
    subprocess.call("zfs destroy -r data/dst", shell=True)

    create_dataset("data/src")
    create_dataset("data/src/child")
    subprocess.check_call("dd if=/dev/urandom of=/mnt/data/src/blob bs=1M count=1", shell=True)
    subprocess.check_call("dd if=/dev/urandom of=/mnt/data/src/child/blob bs=1M count=1", shell=True)
    subprocess.check_call("zfs snapshot -r data/src@2018-10-01_01-00", shell=True)

    subprocess.check_call("(zfs send -R data/src@2018-10-01_01-00 | throttle -b 102400 | zfs recv -s -F data/dst) & "
                          f"sleep {kill_timeout}; killall zfs", shell=True)
    assert "receive_resume_token\t1-" in subprocess.check_output("zfs get -H receive_resume_token data/dst",
                                                                 shell=True, encoding="utf-8")

    definition = yaml.safe_load(textwrap.dedent("""\
        timezone: "UTC"

        periodic-snapshot-tasks:
          src:
            dataset: data/src
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
            source-dataset: data/src
            target-dataset: data/dst
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
    assert len(list_snapshots(local_shell, "data/dst", False)) == 1
    assert len(list_snapshots(local_shell, "data/dst/child", False)) == 1
