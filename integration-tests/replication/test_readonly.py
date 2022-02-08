# -*- coding=utf-8 -*-
import subprocess
import textwrap

import pytest
import yaml

from zettarepl.utils.test import run_replication_test, transports


@pytest.mark.parametrize("readonly", ["ignore", "set", "require"])
@pytest.mark.parametrize("dataset_ops", [
    (
        [],
        "Target dataset 'data/dst' exists and does not have readonly=on property, but replication task is set up "
        "to require this property. Refusing to replicate."
    ),
    (
        [("data/dst", "on"), ("data/dst/sub1", "off")],
        "Target dataset 'data/dst/sub1' exists and does not have readonly=on property, but replication task is set up "
        "to require this property. Refusing to replicate."
    ),
    (
        [("data/dst", "on")],
        None
    ),
])
def test_readonly(readonly, dataset_ops):
    dataset_ops, error = dataset_ops

    if dataset_ops and readonly == "ignore":
        return

    subprocess.call("zfs destroy -r data/src", shell=True)
    subprocess.call("zfs receive -A data/dst", shell=True)
    subprocess.call("zfs destroy -r data/dst", shell=True)

    subprocess.check_call("zfs create data/src", shell=True)
    subprocess.check_call("zfs create data/src/sub1", shell=True)
    subprocess.check_call("zfs snapshot -r data/src@2018-10-01_01-00", shell=True)
    subprocess.check_call("zfs snapshot -r data/src@2018-10-02_01-00", shell=True)

    subprocess.check_call("zfs send -R data/src@2018-10-01_01-00 | zfs recv data/dst", shell=True)

    for dataset, op in dataset_ops:
        subprocess.check_call(f"zfs set readonly={op} {dataset}", shell=True)

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
    definition["replication-tasks"]["src"]["readonly"] = readonly

    if readonly == "require" and error is not None:
        e = run_replication_test(definition, success=False)

        assert e.error == error
    else:
        run_replication_test(definition)

        if readonly == "ignore":
            assert subprocess.check_output(
                "zfs get -H -o value readonly data/dst/sub1", shell=True, encoding="utf-8"
            ) == "off\n"
        else:
            assert subprocess.check_output(
                "zfs get -H -o value,source readonly data/dst", shell=True, encoding="utf-8"
            ) == "on\tlocal\n"
            assert subprocess.check_output(
                "zfs get -H -o value,source readonly data/dst/sub1", shell=True, encoding="utf-8"
            ) == "on\tinherited from data/dst\n"


@pytest.mark.parametrize("readonly", ["ignore", "set", "require"])
def test_readonly_dst_does_not_exist(readonly):
    subprocess.call("zfs destroy -r data/src", shell=True)
    subprocess.call("zfs receive -A data/dst", shell=True)
    subprocess.call("zfs destroy -r data/dst", shell=True)

    subprocess.check_call("zfs create data/src", shell=True)
    subprocess.check_call("zfs create data/src/sub1", shell=True)
    subprocess.check_call("zfs snapshot -r data/src@2018-10-01_01-00", shell=True)
    subprocess.check_call("zfs snapshot -r data/src@2018-10-02_01-00", shell=True)

    definition = yaml.safe_load(textwrap.dedent("""\
        timezone: "UTC"

        replication-tasks:
          src:
            direction: push
            transport:
              type: local
            source-dataset: data/src
            target-dataset: data/dst/child
            recursive: true
            also-include-naming-schema:
              - "%Y-%m-%d_%H-%M"
            auto: false
            retention-policy: none
            retries: 1
    """))
    definition["replication-tasks"]["src"]["readonly"] = readonly

    run_replication_test(definition)

    if readonly == "ignore":
        assert subprocess.check_output(
            "zfs get -H -o value readonly data/dst/child/sub1", shell=True, encoding="utf-8"
        ) == "off\n"
    else:
        assert subprocess.check_output(
            "zfs get -H -o value,source readonly data/dst/child", shell=True, encoding="utf-8"
        ) == "on\tlocal\n"
        assert subprocess.check_output(
            "zfs get -H -o value,source readonly data/dst/child/sub1", shell=True, encoding="utf-8"
        ) == "on\tinherited from data/dst/child\n"


def test_readonly_require_zvol():
    subprocess.call("zfs destroy -r data/src", shell=True)
    subprocess.call("zfs receive -A data/dst", shell=True)
    subprocess.call("zfs destroy -r data/dst", shell=True)

    subprocess.check_call("zfs create -V 1M data/src", shell=True)
    subprocess.check_call("zfs snapshot -r data/src@2018-10-01_01-00", shell=True)

    subprocess.check_call("zfs create -V 1M data/dst", shell=True)

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
            readonly: require
            retention-policy: none
            retries: 1
    """))
    error = run_replication_test(definition, success=False)

    assert error.error == (
        "Target dataset 'data/dst' exists and does not have readonly=on property, but replication task is set up to "
        "require this property. Refusing to replicate. Please run \"zfs set readonly=on data/dst\" on the target "
        "system to fix this."
    )


@pytest.mark.parametrize("transport", transports(netcat=False, unprivileged=True))
def test_unprivileged_readonly_set(transport):
    subprocess.call("zfs destroy -r data/src", shell=True)
    subprocess.call("zfs receive -A data/dst", shell=True)
    subprocess.call("zfs destroy -r data/dst", shell=True)

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
            direction: push
            source-dataset: data/src
            target-dataset: data/dst/dst
            recursive: false
            readonly: set
            also-include-naming-schema:
              - "%Y-%m-%d_%H-%M"
            auto: false
            retention-policy: none
            retries: 2
    """))
    definition["replication-tasks"]["src"]["transport"] = transport

    assert run_replication_test(definition, success=False).error == (
        "cannot set `readonly` property for 'data/dst/dst': permission denied. Please either allow your replication "
        "user to change dataset properties or set `readonly` replication task option to `IGNORE`"
    )
