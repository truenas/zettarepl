# -*- coding=utf-8 -*-
import subprocess
import textwrap

import pytest
import yaml

from zettarepl.utils.test import run_replication_test, transports


@pytest.mark.parametrize("transport", transports(netcat=False, unprivileged=True))
def test_property_receive(transport):
    subprocess.call("zfs destroy -r tank/src", shell=True)
    subprocess.call("zfs receive -A tank/dst", shell=True)
    subprocess.call("zfs destroy -r tank/dst", shell=True)

    subprocess.check_call("zfs create tank/src", shell=True)
    subprocess.check_call("zfs snapshot -r tank/src@2021-03-10_12-00", shell=True)
    subprocess.check_call("zfs set truenas:customproperty=1 tank/src", shell=True)
    subprocess.check_call("zfs snapshot -r tank/src@2021-03-10_12-01", shell=True)

    subprocess.check_call("zfs create tank/dst", shell=True)
    subprocess.check_call("zfs create tank/dst/dst", shell=True)
    subprocess.check_call("zfs allow user receive,create,mount tank/dst/dst", shell=True)
    subprocess.check_call("zfs send tank/src@2021-03-10_12-00 | zfs recv -s -F tank/dst/dst", shell=True)
    subprocess.check_call("zfs umount tank/dst/dst", shell=True)
    subprocess.check_call("chown user:user /mnt/tank/dst/dst", shell=True)

    definition = yaml.safe_load(textwrap.dedent("""\
        timezone: "UTC"

        replication-tasks:
          src:
            direction: push
            source-dataset: tank/src
            target-dataset: tank/dst/dst
            recursive: false
            properties: true
            also-include-naming-schema:
              - "%Y-%m-%d_%H-%M"
            auto: false
            retention-policy: none
            retries: 2
    """))
    definition["replication-tasks"]["src"]["transport"] = transport

    warning = "cannot receive truenas:customproperty property on tank/dst/dst: permission denied"

    assert warning in run_replication_test(definition).warnings

    subprocess.check_call("zfs snapshot -r tank/src@2021-03-10_12-02", shell=True)

    assert warning in run_replication_test(definition).warnings
