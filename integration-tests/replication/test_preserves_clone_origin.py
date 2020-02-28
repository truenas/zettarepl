# -*- coding=utf-8 -*-
import subprocess
import textwrap
from unittest.mock import Mock

import yaml

from zettarepl.definition.definition import Definition
from zettarepl.replication.task.task import ReplicationTask
from zettarepl.transport.local import LocalShell
from zettarepl.utils.itertools import select_by_class
from zettarepl.utils.test import run_replication_test
from zettarepl.zettarepl import Zettarepl


def test_preserves_clone_origin():
    subprocess.call("zfs destroy -r data/src", shell=True)
    subprocess.call("zfs receive -A data/dst", shell=True)
    subprocess.call("zfs destroy -r data/dst", shell=True)

    subprocess.check_call("zfs create data/src", shell=True)
    subprocess.check_call("zfs create data/src/iocage", shell=True)
    subprocess.check_call("zfs create data/src/iocage/child", shell=True)
    subprocess.check_call("zfs create data/src/iocage/child/dataset", shell=True)
    subprocess.check_call("dd if=/dev/urandom of=/mnt/data/src/iocage/child/dataset/blob bs=1M count=1", shell=True)
    subprocess.check_call("zfs snapshot -r data/src@2019-11-08_14-00", shell=True)
    subprocess.check_call("zfs create data/src/iocage/another", shell=True)
    subprocess.check_call("zfs create data/src/iocage/another/child", shell=True)
    subprocess.check_call("zfs clone data/src/iocage/child/dataset@2019-11-08_14-00 "
                          "data/src/iocage/another/child/clone", shell=True)
    subprocess.check_call("zfs snapshot -r data/src@2019-11-08_15-00", shell=True)

    assert (
        subprocess.check_output(
            "zfs get -H origin data/src/iocage/another/child/clone",
            encoding="utf-8", shell=True
        ).split("\n")[0].split("\t")[2] ==
        "data/src/iocage/child/dataset@2019-11-08_14-00"
    )
    assert int(
        subprocess.check_output(
            "zfs get -H -p used data/src/iocage/another/child/clone",
            encoding="utf-8", shell=True
        ).split("\n")[0].split("\t")[2]
    ) < 2e6

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
            properties: true
            replicate: true
            also-include-naming-schema:
              - "%Y-%m-%d_%H-%M"
            auto: false
            retention-policy: none
            retries: 1
    """))

    run_replication_test(definition)

    assert (
        subprocess.check_output(
            "zfs get -H origin data/dst/iocage/another/child/clone",
            encoding="utf-8", shell=True
        ).split("\n")[0].split("\t")[2] ==
        "data/dst/iocage/child/dataset@2019-11-08_14-00"
    )
    assert int(
        subprocess.check_output(
            "zfs get -H -p used data/dst/iocage/another/child/clone",
            encoding="utf-8", shell=True
        ).split("\n")[0].split("\t")[2]
    ) < 2e6
