# -*- coding=utf-8 -*-
import subprocess
import textwrap

import pytest
import yaml

from zettarepl.utils.test import create_dataset, run_replication_test, transports


def test_replication_mount__skip_parent():
    subprocess.call("zfs destroy -r tank/src", shell=True)
    subprocess.call("zfs receive -A tank/dst", shell=True)
    subprocess.call("zfs destroy -r tank/dst", shell=True)

    try:
        create_dataset("tank/src")

        create_dataset("tank/src/UNIX")
        subprocess.check_call("zfs set mountpoint=/UNIX tank/src/UNIX", shell=True)

        create_dataset("tank/src/UNIX/var")
        subprocess.check_call("zfs set mountpoint=/var tank/src/UNIX/var", shell=True)

        create_dataset("tank/src/UNIX/var/audit")

        subprocess.check_call("zfs snapshot -r tank/src@2018-10-01_01-00", shell=True)

        create_dataset("tank/dst")
        create_dataset("tank/dst/server")
        create_dataset("tank/dst/server/UNIX")
        create_dataset("tank/dst/server/UNIX/var")
        subprocess.check_call("zfs set mountpoint=none tank/dst/server/UNIX/var", shell=True)
        create_dataset("tank/dst/server/UNIX/var/audit")
        subprocess.check_call("zfs set mountpoint=/tank/dst/server/var/audit tank/dst/server/UNIX/var/audit", shell=True)
        subprocess.check_call("zfs set readonly=on tank/dst/server", shell=True)

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
                source-dataset: tank/src/UNIX
                target-dataset: tank/dst/server/UNIX
                recursive: true
                properties: false
                periodic-snapshot-tasks:
                  - src
                auto: true
                retention-policy: none
                readonly: set
        """))

        run_replication_test(definition)

        mounted = subprocess.check_output(
            "zfs get -H -o value mounted tank/dst/server/UNIX/var/audit",
            shell=True,
            encoding="utf-8",
        )
        assert mounted == "yes\n"
    finally:
        subprocess.call("zfs destroy -r tank/src", shell=True)


@pytest.mark.parametrize("transport", transports())
def test_replication_mount__no_mount(transport):
    subprocess.call("zfs destroy -r tank/src", shell=True)
    subprocess.call("zfs destroy -r tank/dst", shell=True)

    subprocess.check_call("zfs create tank/src", shell=True)
    subprocess.check_call("zfs create tank/src/child1", shell=True)
    subprocess.check_call("zfs create tank/src/child1/child2", shell=True)
    subprocess.check_call("zfs snapshot -r tank/src@2018-10-01_01-00", shell=True)

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
            mount: false
            periodic-snapshot-tasks:
              - src
            auto: true
            retention-policy: none
            readonly: set
    """))
    definition["replication-tasks"]["src"]["transport"] = transport

    run_replication_test(definition)

    for dataset in ["tank/dst", "tank/dst/child1", "tank/dst/child1/child2"]:
        mounted = subprocess.check_output(
            f"zfs get -H -o value mounted {dataset}",
            shell=True,
            encoding="utf-8",
        )
        assert mounted == "no\n"
