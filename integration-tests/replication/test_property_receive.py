# -*- coding=utf-8 -*-
import subprocess
import textwrap

import pytest
import yaml

from zettarepl.utils.test import run_replication_test, transports


@pytest.mark.parametrize("transport", transports(netcat=False, unprivileged=True))
def test_property_receive(transport):
    subprocess.call("zfs destroy -r data/src", shell=True)
    subprocess.call("zfs receive -A data/dst", shell=True)
    subprocess.call("zfs destroy -r data/dst", shell=True)

    transport["username"] = "user"

    subprocess.check_call("zfs create data/src", shell=True)
    subprocess.check_call("zfs snapshot -r data/src@2021-03-10_12-00", shell=True)
    subprocess.check_call("zfs set truenas:customproperty=1 data/src", shell=True)
    subprocess.check_call("zfs snapshot -r data/src@2021-03-10_12-01", shell=True)

    subprocess.check_call("zfs create data/dst", shell=True)
    subprocess.check_call("zfs create data/dst/dst", shell=True)
    subprocess.check_call("zfs allow user receive,create,mount data/dst/dst", shell=True)
    subprocess.check_call("zfs send data/src@2021-03-10_12-00 | zfs recv -s -F data/dst/dst", shell=True)
    subprocess.check_call("zfs umount data/dst/dst", shell=True)
    subprocess.check_call("chown user:user /mnt/data/dst/dst", shell=True)

    definition = yaml.safe_load(textwrap.dedent("""\
        timezone: "UTC"

        replication-tasks:
          src:
            direction: push
            source-dataset: data/src
            target-dataset: data/dst/dst
            recursive: false
            properties: true
            also-include-naming-schema:
              - "%Y-%m-%d_%H-%M"
            auto: false
            retention-policy: none
            retries: 2
    """))
    definition["replication-tasks"]["src"]["transport"] = transport

    assert "cannot receive truenas:customproperty property" in run_replication_test(definition, success=False).error

    subprocess.check_call("zfs snapshot -r data/src@2021-03-10_12-02", shell=True)

    assert "cannot receive truenas:customproperty property" in run_replication_test(definition, success=False).error
