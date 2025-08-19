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
        "Target dataset 'tank/dst' exists and does not have readonly=on property, but replication task is set up "
        "to require this property. Refusing to replicate."
    ),
    (
        [("tank/dst", "on"), ("tank/dst/sub1", "off")],
        "Target dataset 'tank/dst/sub1' exists and does not have readonly=on property, but replication task is set up "
        "to require this property. Refusing to replicate."
    ),
    (
        [("tank/dst", "on")],
        None
    ),
])
def test_readonly(readonly, dataset_ops):
    dataset_ops, error = dataset_ops

    if dataset_ops and readonly == "ignore":
        return

    subprocess.call("zfs destroy -r tank/src", shell=True)
    subprocess.call("zfs receive -A tank/dst", shell=True)
    subprocess.call("zfs destroy -r tank/dst", shell=True)

    subprocess.check_call("zfs create tank/src", shell=True)
    subprocess.check_call("zfs create tank/src/sub1", shell=True)
    subprocess.check_call("zfs snapshot -r tank/src@2018-10-01_01-00", shell=True)
    subprocess.check_call("zfs snapshot -r tank/src@2018-10-02_01-00", shell=True)

    subprocess.check_call("zfs send -R tank/src@2018-10-01_01-00 | zfs recv tank/dst", shell=True)

    for dataset, op in dataset_ops:
        subprocess.check_call(f"zfs set readonly={op} {dataset}", shell=True)

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
                "zfs get -H -o value readonly tank/dst/sub1", shell=True, encoding="utf-8"
            ) == "off\n"
        else:
            assert subprocess.check_output(
                "zfs get -H -o value,source readonly tank/dst", shell=True, encoding="utf-8"
            ) == "on\tlocal\n"
            assert subprocess.check_output(
                "zfs get -H -o value,source readonly tank/dst/sub1", shell=True, encoding="utf-8"
            ) == "on\tinherited from tank/dst\n"


@pytest.mark.parametrize("readonly", ["ignore", "set", "require"])
def test_readonly_dst_does_not_exist(readonly):
    subprocess.call("zfs destroy -r tank/src", shell=True)
    subprocess.call("zfs receive -A tank/dst", shell=True)
    subprocess.call("zfs destroy -r tank/dst", shell=True)

    subprocess.check_call("zfs create tank/src", shell=True)
    subprocess.check_call("zfs create tank/src/sub1", shell=True)
    subprocess.check_call("zfs snapshot -r tank/src@2018-10-01_01-00", shell=True)
    subprocess.check_call("zfs snapshot -r tank/src@2018-10-02_01-00", shell=True)

    definition = yaml.safe_load(textwrap.dedent("""\
        timezone: "UTC"

        replication-tasks:
          src:
            direction: push
            transport:
              type: local
            source-dataset: tank/src
            target-dataset: tank/dst/child
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
            "zfs get -H -o value readonly tank/dst/child/sub1", shell=True, encoding="utf-8"
        ) == "off\n"
    else:
        assert subprocess.check_output(
            "zfs get -H -o value,source readonly tank/dst/child", shell=True, encoding="utf-8"
        ) == "on\tlocal\n"
        assert subprocess.check_output(
            "zfs get -H -o value,source readonly tank/dst/child/sub1", shell=True, encoding="utf-8"
        ) == "on\tinherited from tank/dst/child\n"


def test_readonly_require_zvol():
    subprocess.call("zfs destroy -r tank/src", shell=True)
    subprocess.call("zfs receive -A tank/dst", shell=True)
    subprocess.call("zfs destroy -r tank/dst", shell=True)

    subprocess.check_call("zfs create -V 1M tank/src", shell=True)
    subprocess.check_call("zfs snapshot -r tank/src@2018-10-01_01-00", shell=True)

    subprocess.check_call("zfs create -V 1M tank/dst", shell=True)

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
            also-include-naming-schema:
              - "%Y-%m-%d_%H-%M"
            auto: false
            readonly: require
            retention-policy: none
            retries: 1
    """))
    error = run_replication_test(definition, success=False)

    assert error.error == (
        "Target dataset 'tank/dst' exists and does not have readonly=on property, but replication task is set up to "
        "require this property. Refusing to replicate. Please run \"zfs set readonly=on tank/dst\" on the target "
        "system to fix this."
    )


@pytest.mark.parametrize("transport", transports(netcat=False, unprivileged=True))
def test_unprivileged_readonly_set(transport):
    subprocess.call("zfs destroy -r tank/src", shell=True)
    subprocess.call("zfs receive -A tank/dst", shell=True)
    subprocess.call("zfs destroy -r tank/dst", shell=True)

    subprocess.check_call("zfs create tank/src", shell=True)
    subprocess.check_call("zfs snapshot -r tank/src@2021-03-10_12-00", shell=True)

    subprocess.check_call("zfs create tank/dst", shell=True)
    subprocess.check_call("zfs create tank/dst/dst", shell=True)
    subprocess.check_call("zfs allow user receive,create,mount tank/dst", shell=True)
    subprocess.check_call("zfs allow user receive,create,mount tank/dst/dst", shell=True)
    subprocess.check_call("chown -R user:user /mnt/tank/dst", shell=True)
    subprocess.check_call("zfs umount tank/dst/dst", shell=True)

    definition = yaml.safe_load(textwrap.dedent("""\
        timezone: "UTC"

        replication-tasks:
          src:
            direction: push
            source-dataset: tank/src
            target-dataset: tank/dst/dst
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
        "cannot set `readonly` property for 'tank/dst/dst': permission denied. Please either allow your replication "
        "user to change dataset properties or set `readonly` replication task option to `IGNORE`"
    )
