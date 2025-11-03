# -*- coding=utf-8 -*-
import os
import subprocess
import textwrap

import pytest
import yaml

from zettarepl.utils.test import create_dataset, run_replication_test, transports


@pytest.mark.parametrize("has_data", [True, False])
def test_encrypted_target(has_data):
    subprocess.call("zfs destroy -r tank/src", shell=True)
    subprocess.call("zfs destroy -r tank/dst_parent", shell=True)

    create_dataset("tank/src")
    subprocess.check_call("zfs snapshot tank/src@2018-10-01_01-00", shell=True)
    subprocess.check_call("zfs snapshot tank/src@2018-10-01_02-00", shell=True)

    create_dataset("tank/dst_parent", True)
    subprocess.check_call("zfs create tank/dst_parent/dst", shell=True)

    if has_data:
        with open("/mnt/tank/dst_parent/dst/file", "w"):
            pass

    definition = yaml.safe_load(textwrap.dedent("""\
        timezone: "UTC"

        replication-tasks:
          src:
            direction: push
            transport:
              type: local
            source-dataset: tank/src
            target-dataset: tank/dst_parent/dst
            recursive: true
            also-include-naming-schema:
              - "%Y-%m-%d_%H-%M"
            auto: false
            retention-policy: none
            retries: 1
    """))
    error = run_replication_test(definition, success=not has_data)

    if has_data:
        assert "does not have matching snapshots but has data" in error.error


def test_encrypted_target_local():
    subprocess.call("zfs destroy -r tank/src", shell=True)
    subprocess.call("zfs destroy -r tank/dst", shell=True)

    create_dataset("tank/src")
    subprocess.check_call("zfs snapshot tank/src@2018-10-01_01-00", shell=True)
    subprocess.check_call("zfs snapshot tank/src@2018-10-01_02-00", shell=True)

    create_dataset("tank/dst", True)

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
    assert "is its own encryption root" in error.error


@pytest.mark.parametrize("encryption", [
    {"key": "aa" * 32, "key-format": "hex"},
    {"key": "password", "key-format": "passphrase"},
])
@pytest.mark.parametrize("key_location", ["$TrueNAS", "/tmp/test.key"])
@pytest.mark.parametrize("transport", transports())
def test_create_encrypted_target(encryption, key_location, transport):
    encryption["key-location"] = key_location

    subprocess.call("zfs destroy -r tank/src", shell=True)
    subprocess.call("zfs destroy -r tank/dst", shell=True)
    if os.path.exists("/tmp/test.key"):
        os.unlink("/tmp/test.key")

    create_dataset("tank/src", encrypted=True)
    subprocess.check_call("zfs snapshot tank/src@2018-10-01_01-00", shell=True)
    subprocess.check_call("zfs snapshot tank/src@2018-10-01_02-00", shell=True)

    definition = yaml.safe_load(textwrap.dedent("""\
        timezone: "UTC"

        replication-tasks:
          src:
            direction: push
            source-dataset: tank/src
            target-dataset: tank/dst
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
                subprocess.check_output(["midclt", "call", "--job", "pool.dataset.export_key", "tank/dst"]).decode().strip() ==
                encryption["key"]
            )
    else:
        assert (
            subprocess.check_output("zfs get -H -o value keylocation tank/dst", shell=True).decode().strip() ==
            f'file://{encryption["key-location"]}'
        )


def test_encrypted_source_but_unencrypted_target_exists():
    subprocess.call("zfs destroy -r tank/src", shell=True)
    subprocess.call("zfs destroy -r tank/dst", shell=True)

    create_dataset("tank/src", encrypted=True)
    subprocess.check_call("zfs snapshot tank/src@2018-10-01_01-00", shell=True)
    subprocess.check_call("zfs snapshot tank/src@2018-10-01_02-00", shell=True)

    create_dataset("tank/dst")

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
    error = run_replication_test(definition, success=False)
    assert "but it already exists and is not encrypted" in error.error


def test_encrypted_target_replication_from_scratch():
    subprocess.call("zfs destroy -r tank/src", shell=True)
    subprocess.call("zfs destroy -r tank/dst", shell=True)

    create_dataset("tank/src", True)
    subprocess.check_call("zfs snapshot tank/src@2018-10-01_02-00", shell=True)

    create_dataset("tank/dst", True)
    subprocess.check_call("zfs snapshot tank/dst@2018-10-01_01-00", shell=True)

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
            allow-from-scratch: true
            retention-policy: none
            retries: 1
    """))
    run_replication_test(definition)


def test_re_encrypt_preserving_properties():
    subprocess.call("zfs destroy -r tank/src", shell=True)
    subprocess.call("zfs destroy -r tank/dst", shell=True)

    create_dataset("tank/src", encrypted=True)
    subprocess.check_call("zfs snapshot tank/src@2018-10-01_01-00", shell=True)

    create_dataset("tank/dst")

    definition = yaml.safe_load(textwrap.dedent("""\
        timezone: "UTC"

        replication-tasks:
          src:
            direction: push
            transport:
              type: local
            source-dataset: tank/src
            target-dataset: tank/dst/target
            recursive: true
            properties: true
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
    error = run_replication_test(definition, success=False)
    assert error.error.startswith("Re-encrypting already encrypted source dataset 'tank/src'")


@pytest.mark.parametrize("encryption", [
    {"key": "aa" * 32, "key-format": "hex"},
    {"key": "password", "key-format": "passphrase"},
])
def test_encrypt_recursive(encryption):
    subprocess.call("zfs destroy -r tank/src", shell=True)
    subprocess.call("zfs destroy -r tank/dst", shell=True)

    create_dataset("tank/src")
    create_dataset("tank/src/child")
    subprocess.check_call("zfs snapshot -r tank/src@2018-10-01_01-00", shell=True)

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
            encryption:
              key-location: $TrueNAS
            also-include-naming-schema:
              - "%Y-%m-%d_%H-%M"
            auto: false
            retention-policy: none
            retries: 1
    """))
    definition["replication-tasks"]["src"]["encryption"].update(**encryption)

    run_replication_test(definition)

    assert (
        subprocess.check_output("zfs get -H -o value keyformat tank/dst", shell=True).decode().strip() ==
        f'{encryption["key-format"]}'
    )
    assert (
        subprocess.check_output("zfs get -H -o value keyformat tank/dst/child", shell=True).decode().strip() ==
        f'{encryption["key-format"]}'
    )
