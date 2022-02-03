# -*- coding=utf-8 -*-
import os
import subprocess
import textwrap

import pytest
import yaml

from zettarepl.utils.test import run_replication_test, transports


@pytest.mark.parametrize("direction", ["push", "pull"])
@pytest.mark.parametrize("shell", ["/bin/sh", "/bin/csh"])
@pytest.mark.parametrize("transport", transports(netcat=False, unprivileged=True))
def test_shells(direction, shell, transport):
    subprocess.call("zfs destroy -r data/src", shell=True)
    subprocess.call("zfs receive -A data/dst", shell=True)
    subprocess.call("zfs destroy -r data/dst", shell=True)

    if not os.path.exists(shell):
        pytest.skip(f"{shell} does not exist")

    subprocess.check_call(["chsh", "-s", shell, "user"])

    subprocess.check_call("zfs create data/src", shell=True)
    subprocess.check_call("zfs snapshot -r data/src@2021-03-10_12-00", shell=True)

    subprocess.check_call("zfs create data/dst", shell=True)
    subprocess.check_call("zfs create data/dst/dst", shell=True)
    subprocess.check_call("zfs allow user receive,create,mount data/dst", shell=True)
    subprocess.check_call("zfs allow user receive,create,mount data/dst/dst", shell=True)
    subprocess.check_call("chown -R user:user /mnt/data/dst", shell=True)
    subprocess.check_call("zfs umount data/dst/dst", shell=True)

    definition = yaml.safe_load(textwrap.dedent("""\
        timezone: "UTC"

        replication-tasks:
          src:
            source-dataset: data/src
            target-dataset: data/dst/dst
            recursive: false
            properties: true
            auto: false
            retention-policy: none
            retries: 2
    """))
    definition["replication-tasks"]["src"]["direction"] = direction
    definition["replication-tasks"]["src"]["transport"] = transport
    if direction == "push":
        definition["replication-tasks"]["src"]["also-include-naming-schema"] = ["%Y-%m-%d_%H-%M"]
    else:
        definition["replication-tasks"]["src"]["naming-schema"] = ["%Y-%m-%d_%H-%M"]

    run_replication_test(definition)
