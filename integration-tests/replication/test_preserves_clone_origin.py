# -*- coding=utf-8 -*-
import subprocess
import textwrap

import yaml

from zettarepl.utils.test import run_replication_test


def test_preserves_clone_origin():
    subprocess.call("zfs destroy -r tank/src", shell=True)
    subprocess.call("zfs receive -A tank/dst", shell=True)
    subprocess.call("zfs destroy -r tank/dst", shell=True)

    subprocess.check_call("zfs create tank/src", shell=True)
    subprocess.check_call("zfs create tank/src/iocage", shell=True)
    subprocess.check_call("zfs create tank/src/iocage/child", shell=True)
    subprocess.check_call("zfs create tank/src/iocage/child/dataset", shell=True)
    subprocess.check_call("dd if=/dev/urandom of=/mnt/tank/src/iocage/child/dataset/blob bs=1M count=1", shell=True)
    subprocess.check_call("zfs snapshot -r tank/src@2019-11-08_14-00", shell=True)
    subprocess.check_call("zfs create tank/src/iocage/another", shell=True)
    subprocess.check_call("zfs create tank/src/iocage/another/child", shell=True)
    subprocess.check_call("zfs clone tank/src/iocage/child/dataset@2019-11-08_14-00 "
                          "tank/src/iocage/another/child/clone", shell=True)
    subprocess.check_call("zfs snapshot -r tank/src@2019-11-08_15-00", shell=True)

    assert (
        subprocess.check_output(
            "zfs get -H origin tank/src/iocage/another/child/clone",
            encoding="utf-8", shell=True
        ).split("\n")[0].split("\t")[2] ==
        "tank/src/iocage/child/dataset@2019-11-08_14-00"
    )
    assert int(
        subprocess.check_output(
            "zfs get -H -p used tank/src/iocage/another/child/clone",
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
            source-dataset: tank/src
            target-dataset: tank/dst
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
            "zfs get -H origin tank/dst/iocage/another/child/clone",
            encoding="utf-8", shell=True
        ).split("\n")[0].split("\t")[2] ==
        "tank/dst/iocage/child/dataset@2019-11-08_14-00"
    )
    assert int(
        subprocess.check_output(
            "zfs get -H -p used tank/dst/iocage/another/child/clone",
            encoding="utf-8", shell=True
        ).split("\n")[0].split("\t")[2]
    ) < 2e6
