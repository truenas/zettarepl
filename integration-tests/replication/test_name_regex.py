# -*- coding=utf-8 -*-
import logging
import subprocess
import textwrap

import pytest
import yaml

from zettarepl.snapshot.list import list_snapshots
from zettarepl.transport.local import LocalShell
from zettarepl.utils.test import transports, create_dataset, run_replication_test


@pytest.mark.parametrize("transport", transports())
@pytest.mark.parametrize("all_names", [True, False])
@pytest.mark.parametrize("resume", [False, True])
def test_name_regex(caplog, transport, all_names, resume):
    subprocess.call("zfs destroy -r data/src", shell=True)
    subprocess.call("zfs destroy -r data/dst", shell=True)

    create_dataset("data/src")
    subprocess.check_call("zfs snapshot -r data/src@snap-2", shell=True)
    subprocess.check_call("zfs snapshot -r data/src@manual-1", shell=True)
    subprocess.check_call("zfs snapshot -r data/src@snap-1", shell=True)
    subprocess.check_call("zfs snapshot -r data/src@manual-2", shell=True)
    subprocess.check_call("zfs snapshot -r data/src@snap-3", shell=True)

    if resume:
        subprocess.check_call("zfs send data/src@snap-2 | zfs recv -s -F data/dst", shell=True)

    definition = yaml.safe_load(textwrap.dedent("""\
        timezone: "UTC"

        replication-tasks:
          src:
            direction: push
            source-dataset: data/src
            target-dataset: data/dst
            recursive: false
            auto: false
            retention-policy: none
            retries: 1
    """))
    definition["replication-tasks"]["src"]["transport"] = transport
    if all_names:
        definition["replication-tasks"]["src"]["name-regex"] = ".*"
    else:
        definition["replication-tasks"]["src"]["name-regex"] = "snap-.*"

    caplog.set_level(logging.INFO)
    run_replication_test(definition)

    assert len(list_snapshots(LocalShell(), "data/dst", False)) == (5 if all_names else 3)

    logs = [record.message
            for record in caplog.get_records("call")
            if "For replication task 'src': doing push" in record.message]
    if all_names:
        if resume:
            assert len(logs) == 1
        else:
            assert len(logs) == 2
    else:
        if resume:
            assert len(logs) == 2
        else:
            assert len(logs) == 3
