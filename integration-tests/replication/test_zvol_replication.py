# -*- coding=utf-8 -*-
import subprocess
import textwrap
import time

import pytest
import yaml

from zettarepl.snapshot.list import list_snapshots
from zettarepl.transport.local import LocalShell
from zettarepl.utils.test import create_dataset, run_replication_test


@pytest.mark.parametrize("as_root", [True, False])
def test_zvol_replication(as_root):
    subprocess.call("zfs destroy -r data/src", shell=True)
    subprocess.call("zfs receive -A data/dst", shell=True)
    subprocess.call("zfs destroy -r data/dst", shell=True)

    if as_root:
        subprocess.check_call("zfs create -V 1M data/src", shell=True)
    else:
        subprocess.check_call("zfs create data/src", shell=True)
        subprocess.check_call("zfs create -V 1M data/src/zvol", shell=True)
    subprocess.check_call("zfs snapshot -r data/src@2018-10-01_01-00", shell=True)
    subprocess.check_call("zfs snapshot -r data/src@2018-10-01_02-00", shell=True)

    definition = yaml.safe_load(textwrap.dedent("""\
        timezone: "UTC"

        replication-tasks:
          src:
            direction: push
            transport:
              type: local
            source-dataset: data/src
            target-dataset: data/dst
            recursive: true
            also-include-naming-schema:
              - "%Y-%m-%d_%H-%M"
            auto: false
            retention-policy: none
            retries: 1
    """))
    run_replication_test(definition)

    local_shell = LocalShell()
    assert len(list_snapshots(local_shell, "data/dst", False)) == 2
    if not as_root:
        assert len(list_snapshots(local_shell, "data/dst/zvol", False)) == 2


def test_zvol_replication__onto_existing_dataset():
    subprocess.call("zfs destroy -r data/src", shell=True)
    subprocess.call("zfs receive -A data/dst", shell=True)
    subprocess.call("zfs destroy -r data/dst", shell=True)

    subprocess.check_call("zfs create -V 1M data/src", shell=True)
    subprocess.check_call("zfs snapshot -r data/src@2018-10-01_01-00", shell=True)
    subprocess.check_call("zfs snapshot -r data/src@2018-10-01_02-00", shell=True)

    subprocess.check_call("zfs create data/dst", shell=True)

    definition = yaml.safe_load(textwrap.dedent("""\
        timezone: "UTC"

        replication-tasks:
          src:
            direction: push
            transport:
              type: local
            source-dataset: data/src
            target-dataset: data/dst
            recursive: true
            also-include-naming-schema:
              - "%Y-%m-%d_%H-%M"
            auto: false
            retention-policy: none
            retries: 1
    """))
    error = run_replication_test(definition, success=False)

    assert error.error == "Source 'data/src' is a volume, but target 'data/dst' already exists and is a filesystem"


def test_zvol_replication__onto_existing_encrypted_unrelated_dataset():
    subprocess.call("zfs destroy -r data/src", shell=True)
    subprocess.call("zfs receive -A data/dst", shell=True)
    subprocess.call("zfs destroy -r data/dst", shell=True)

    subprocess.check_call("zfs create -V 5M data/src", shell=True)
    subprocess.check_call("zfs snapshot -r data/src@2018-10-01_01-00", shell=True)
    create_dataset("data/dst", encrypted=True)
    subprocess.check_call("zfs create -V 5M data/dst/vol", shell=True)
    subprocess.check_call("dd if=/dev/urandom of=/dev/zvol/data/dst/vol bs=1M count=5", shell=True)

    definition = yaml.safe_load(textwrap.dedent("""\
        timezone: "UTC"

        replication-tasks:
          src:
            direction: push
            transport:
              type: local
            source-dataset: data/src
            target-dataset: data/dst/vol
            recursive: true
            also-include-naming-schema:
              - "%Y-%m-%d_%H-%M"
            auto: false
            retention-policy: none
            retries: 1
    """))
    error = run_replication_test(definition, success=False)

    assert error.error == "Unable to send dataset 'data/src' to existing unrelated encrypted dataset 'data/dst/vol'"
