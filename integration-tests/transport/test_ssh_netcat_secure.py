# -*- coding=utf-8 -*-
import subprocess
import textwrap

import yaml

from zettarepl.utils.test import run_replication_test, set_localhost_transport_options


def test_ssh_netcat_secure_replication():
    subprocess.call("zfs destroy -r data/src", shell=True)
    subprocess.call("zfs receive -A data/dst", shell=True)
    subprocess.call("zfs destroy -r data/dst", shell=True)

    subprocess.check_call("zfs create data/src", shell=True)
    subprocess.check_call("zfs snapshot data/src@2018-10-01_01-00", shell=True)
    subprocess.check_call("zfs snapshot data/src@2018-10-01_02-00", shell=True)

    subprocess.check_call("zfs create data/dst", shell=True)

    definition = yaml.safe_load(textwrap.dedent("""\
        timezone: "UTC"

        replication-tasks:
          src:
            direction: push
            transport:
              type: ssh+netcat
              active-side: local
              hostname: 127.0.0.1
              secure: true
            source-dataset: data/src
            target-dataset: data/dst
            recursive: true
            also-include-naming-schema:
              - "%Y-%m-%d_%H-%M"
            auto: false
            retention-policy: none
    """))
    set_localhost_transport_options(definition["replication-tasks"]["src"]["transport"])

    run_replication_test(definition)
