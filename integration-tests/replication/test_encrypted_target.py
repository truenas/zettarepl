# -*- coding=utf-8 -*-
import os
import subprocess
import textwrap

import pytest
import yaml

from zettarepl.utils.test import create_dataset, run_replication_test, transports


@pytest.mark.parametrize("has_data", [True, False])
def test_encrypted_target(has_data):
    subprocess.call("zfs destroy -r data/src", shell=True)
    subprocess.call("zfs destroy -r data/dst_parent", shell=True)

    create_dataset("data/src")
    subprocess.check_call("zfs snapshot data/src@2018-10-01_01-00", shell=True)
    subprocess.check_call("zfs snapshot data/src@2018-10-01_02-00", shell=True)

    create_dataset("data/dst_parent", True)
    subprocess.check_call("zfs create data/dst_parent/dst", shell=True)

    if has_data:
        with open("/mnt/data/dst_parent/dst/file", "w"):
            pass

    definition = yaml.safe_load(textwrap.dedent("""\
        timezone: "UTC"

        replication-tasks:
          src:
            direction: push
            transport:
              type: local
            source-dataset: data/src
            target-dataset: data/dst_parent/dst
            recursive: true
            also-include-naming-schema:
              - "%Y-%m-%d_%H-%M"
            auto: false
            retention-policy: none
            retries: 1
    """))
    error = run_replication_test(definition, success=not has_data)

    if has_data:
        assert "does not have snapshots but has data" in error.error


def test_encrypted_target_local():
    subprocess.call("zfs destroy -r data/src", shell=True)
    subprocess.call("zfs destroy -r data/dst", shell=True)

    create_dataset("data/src")
    subprocess.check_call("zfs snapshot data/src@2018-10-01_01-00", shell=True)
    subprocess.check_call("zfs snapshot data/src@2018-10-01_02-00", shell=True)

    create_dataset("data/dst", True)

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
    error = run_replication_test(definition, success=False)
    assert "is its own encryption root" in error.error


@pytest.mark.parametrize("encryption", [
    {"key": "aa" * 32, "key-format": "hex"},
    {"key": "password", "key-format": "passphrase"},
])
@pytest.mark.parametrize("key_location", ["$TrueNAS", "/tmp/test.key"])
@pytest.mark.parametrize("transport", transports())
def test_create_encrypted_target(encryption, key_location, transport):
    encryption["key-location"] = key_location

    subprocess.call("zfs destroy -r data/src", shell=True)
    subprocess.call("zfs destroy -r data/dst", shell=True)
    if os.path.exists("/tmp/test.key"):
        os.unlink("/tmp/test.key")

    create_dataset("data/src", encrypted=True)
    subprocess.check_call("zfs snapshot data/src@2018-10-01_01-00", shell=True)
    subprocess.check_call("zfs snapshot data/src@2018-10-01_02-00", shell=True)

    definition = yaml.safe_load(textwrap.dedent("""\
        timezone: "UTC"

        replication-tasks:
          src:
            direction: push
            source-dataset: data/src
            target-dataset: data/dst
            recursive: true
            properties: false
            also-include-naming-schema:
              - "%Y-%m-%d_%H-%M"
            auto: false
            retention-policy: none
            retries: 1
    """))
    definition["replication-tasks"]["src"]["encryption"] = encryption
    definition["replication-tasks"]["src"]["transport"] = transport
    run_replication_test(definition)

    if key_location == "$TrueNAS":
        if encryption["key-format"] != "passphrase":
            assert (
                subprocess.check_output(["midclt", "call", "-job", "pool.dataset.export_key", "data/dst"]).decode().strip() ==
                encryption["key"]
            )
    else:
        assert (
            subprocess.check_output("zfs get -H -o value keylocation data/dst", shell=True).decode().strip() ==
            f'file://{encryption["key-location"]}'
        )


def test_encrypted_target_but_unencrypted_target_exists():
    subprocess.call("zfs destroy -r data/src", shell=True)
    subprocess.call("zfs destroy -r data/dst", shell=True)

    create_dataset("data/src", encrypted=True)
    subprocess.check_call("zfs snapshot data/src@2018-10-01_01-00", shell=True)
    subprocess.check_call("zfs snapshot data/src@2018-10-01_02-00", shell=True)

    create_dataset("data/dst")

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
            properties: false
            encryption:
              key: password
              key-format: passphrase
              key-location: $TrueNAS
            also-include-naming-schema:
              - "%Y-%m-%d_%H-%M"
            auto: false
            retention-policy: none
            retries: 1
    """))
    error = run_replication_test(definition, False)
    assert "but it already exists and is not encrypted" in error.error


def test_encrypted_target_replication_from_scratch():
    subprocess.call("zfs destroy -r data/src", shell=True)
    subprocess.call("zfs destroy -r data/dst", shell=True)

    create_dataset("data/src", True)
    subprocess.check_call("zfs snapshot data/src@2018-10-01_02-00", shell=True)

    create_dataset("data/dst", True)
    subprocess.check_call("zfs snapshot data/dst@2018-10-01_01-00", shell=True)

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
            allow-from-scratch: true
            retention-policy: none
            retries: 1
    """))
    run_replication_test(definition)
