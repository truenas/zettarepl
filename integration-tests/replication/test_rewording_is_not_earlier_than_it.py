# -*- coding=utf-8 -*-
import os
import subprocess
import textwrap

import pytest
import yaml

from zettarepl.utils.test import run_replication_test, transports


@pytest.mark.parametrize("transport", transports())
@pytest.mark.parametrize("direction", ["push", "pull"])
def test_rewording_is_not_earlier_than_it(transport, direction):
    if transport["type"] == "ssh+netcat":
        uname = os.uname()
        if uname.sysname == "FreeBSD" and uname.release.startswith("12"):
            # FIXME: https://jira.ixsystems.com/browse/NAS-106452
            return

    subprocess.call("zfs destroy -r data/src", shell=True)
    subprocess.call("zfs receive -A data/dst", shell=True)
    subprocess.call("zfs destroy -r data/dst", shell=True)

    subprocess.check_call("zfs create data/src", shell=True)
    subprocess.check_call("zfs snapshot data/src@2018-10-01_01-00", shell=True)
    subprocess.check_call("zfs snapshot data/src@2018-10-01_03-00", shell=True)
    subprocess.check_call("zfs snapshot data/src@2018-10-01_02-00", shell=True)

    definition = yaml.safe_load(textwrap.dedent("""\
        timezone: "UTC"

        replication-tasks:
          src:
            source-dataset: data/src
            target-dataset: data/dst
            recursive: true
            auto: false
            retention-policy: none
            retries: 1
    """))
    definition["replication-tasks"]["src"]["direction"] = direction
    definition["replication-tasks"]["src"]["transport"] = transport
    if direction == "push":
        definition["replication-tasks"]["src"]["also-include-naming-schema"] = "%Y-%m-%d_%H-%M"
    else:
        definition["replication-tasks"]["src"]["naming-schema"] = "%Y-%m-%d_%H-%M"

    error = run_replication_test(definition, success=False)
    assert (
        "is newer than" in error.error and
        "but has an older date" in error.error
    )
