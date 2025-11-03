# -*- coding=utf-8 -*-
import subprocess
import textwrap
import time

import pytest
import yaml

from zettarepl.utils.test import create_dataset, run_replication_test, transports, wait_zvol


@pytest.mark.parametrize("zvol", [False, True])
@pytest.mark.parametrize("mounted", [False, True])
@pytest.mark.parametrize("snapdir", [False, True])
def test_target_without_snapshots_but_with_data(zvol, mounted, snapdir):
    if zvol and (not mounted or snapdir):
        return

    subprocess.call("zfs destroy -r tank/src", shell=True)
    subprocess.call("zfs receive -A tank/dst", shell=True)
    subprocess.call("zfs destroy -r tank/dst", shell=True)

    if zvol:
        subprocess.check_call("zfs create -V 1m tank/src", shell=True)
    else:
        subprocess.check_call("zfs create tank/src", shell=True)
    subprocess.check_call("zfs snapshot -r tank/src@2018-10-01_01-00", shell=True)
    subprocess.check_call("zfs snapshot -r tank/src@2018-10-01_02-00", shell=True)

    if zvol:
        subprocess.check_call("zfs create -V 20m tank/dst", shell=True)
        wait_zvol("/dev/zvol/tank/dst")
        subprocess.check_call("dd if=/dev/urandom of=/dev/zvol/tank/dst bs=15M count=1", shell=True)
    else:
        subprocess.check_call("zfs create tank/dst", shell=True)
        if snapdir:
            subprocess.check_call("zfs set snapdir=visible tank/dst", shell=True)
        if mounted:
            bs = "1K"
        else:
            bs = "15M"
        subprocess.check_call(f"dd if=/dev/urandom of=/mnt/tank/dst/test bs={bs} count=1", shell=True)
        if not mounted:
            subprocess.check_call("zfs umount tank/dst", shell=True)
    time.sleep(5)  # "used" property is not updated immediately

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
    error = run_replication_test(definition, success=False)

    assert "Refusing to overwrite existing data" in error.error


@pytest.mark.parametrize("recursive", [False, True])
@pytest.mark.parametrize("exclude", [False, True])
@pytest.mark.parametrize("src_has_child", [False, True])
@pytest.mark.parametrize("dst_child_mounted", [False, True])
@pytest.mark.parametrize("dst_child_has_own_contents", [False, True])
@pytest.mark.parametrize("deeply_nested", [False, True])
@pytest.mark.parametrize("transport", transports(netcat=False))
def test_replicate_to_existing_dataset_structure(recursive, exclude, src_has_child, dst_child_mounted,
                                                 dst_child_has_own_contents, deeply_nested, transport):
    if not recursive and exclude:
        return
    if dst_child_mounted and dst_child_has_own_contents:
        return

    subprocess.call("zfs destroy -r tank/src", shell=True)
    subprocess.call("zfs receive -A tank/dst", shell=True)
    subprocess.call("zfs destroy -r tank/dst", shell=True)

    src = "tank/src"
    create_dataset(src)
    if src_has_child:
        if deeply_nested:
            src = "tank/src/deep"
            create_dataset(src)
        create_dataset(f"{src}/child")
    subprocess.check_call("zfs snapshot -r tank/src@2018-10-01_01-00", shell=True)

    dst = "tank/dst"
    create_dataset(dst)
    if deeply_nested:
        dst = "tank/dst/deep"
        create_dataset(dst)
    create_dataset(f"{dst}/child")
    if not dst_child_mounted:
        subprocess.check_call(f"zfs umount {dst}/child", shell=True)
        if dst_child_has_own_contents:
            with open(f"/mnt/{dst}/child/file", "w") as f:
                pass

    definition = yaml.safe_load(textwrap.dedent("""\
        timezone: "UTC"

        replication-tasks:
          src:
            direction: push
            transport:
              type: local
            source-dataset: tank/src
            target-dataset: tank/dst
            recursive: false
            also-include-naming-schema:
              - "%Y-%m-%d_%H-%M"
            auto: false
            retention-policy: none
            retries: 1
    """))
    definition["replication-tasks"]["src"]["recursive"] = recursive
    if exclude:
        definition["replication-tasks"]["src"]["exclude"] = [f"{src}/child"]
    definition["replication-tasks"]["src"]["transport"] = transport

    if not recursive or exclude or not src_has_child or dst_child_has_own_contents:
        error = run_replication_test(definition, success=False)

        assert "Refusing to overwrite existing data" in error.error
    else:
        run_replication_test(definition)
