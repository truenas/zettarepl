# -*- coding=utf-8 -*-
import subprocess
import textwrap

import yaml

from zettarepl.utils.test import create_dataset, run_replication_test


def test_replication_mount__skip_parent():
    subprocess.call("zfs destroy -r data/src", shell=True)
    subprocess.call("zfs receive -A data/dst", shell=True)
    subprocess.call("zfs destroy -r data/dst", shell=True)

    try:
        create_dataset("data/src")

        create_dataset("data/src/UNIX")
        subprocess.check_call("zfs set mountpoint=/UNIX data/src/UNIX", shell=True)

        create_dataset("data/src/UNIX/var")
        subprocess.check_call("zfs set mountpoint=/var data/src/UNIX/var", shell=True)

        create_dataset("data/src/UNIX/var/audit")

        subprocess.check_call("zfs snapshot -r data/src@2018-10-01_01-00", shell=True)

        create_dataset("data/dst")
        create_dataset("data/dst/server")
        create_dataset("data/dst/server/UNIX")
        create_dataset("data/dst/server/UNIX/var")
        subprocess.check_call("zfs set mountpoint=none data/dst/server/UNIX/var", shell=True)
        create_dataset("data/dst/server/UNIX/var/audit")
        subprocess.check_call("zfs set mountpoint=/data/dst/server/var/audit data/dst/server/UNIX/var/audit", shell=True)
        subprocess.check_call("zfs set readonly=on data/dst/server", shell=True)

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
                source-dataset: data/src/UNIX
                target-dataset: data/dst/server/UNIX
                recursive: true
                properties: false
                periodic-snapshot-tasks:
                  - src
                auto: true
                retention-policy: none
                readonly: set
        """))

        run_replication_test(definition)

        mounted = subprocess.check_output("zfs get -H -o value mounted data/dst/server/UNIX/var/audit", shell=True, encoding="utf-8")
        assert mounted == "yes\n"
    finally:
        subprocess.call("zfs destroy -r data/src", shell=True)
