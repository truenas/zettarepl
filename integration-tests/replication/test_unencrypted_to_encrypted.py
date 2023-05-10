# -*- coding=utf-8 -*-
import subprocess
import textwrap

import pytest
import yaml

from zettarepl.utils.test import create_dataset, run_replication_test, transports


@pytest.mark.parametrize("transport", transports())
@pytest.mark.parametrize("properties", [False, True])
@pytest.mark.parametrize("encryption", [
    None,
    {
        "key": "password",
        "key-format": "passphrase",
        "key-location": "$TrueNAS",
    },
    "inherit",
])
@pytest.mark.parametrize("source_encrypted", [False, True])
def test_unencrypted_to_encrypted(transport, properties, encryption, source_encrypted):
    if properties and encryption and source_encrypted:
        # Re-encrypting already encrypted source dataset 'data/src' while preserving its properties is not supported
        return

    subprocess.call("zfs destroy -r data/src", shell=True)
    subprocess.call("zfs destroy -r data/dst", shell=True)

    create_dataset("data/src", encrypted=source_encrypted)
    subprocess.check_call("zfs snapshot -r data/src@2018-10-01_01-00", shell=True)

    create_dataset("data/dst", encrypted=True)

    definition = yaml.safe_load(textwrap.dedent("""\
        timezone: "UTC"

        replication-tasks:
          src:
            direction: push
            source-dataset: data/src
            target-dataset: data/dst/child/grandchild
            recursive: false
            also-include-naming-schema:
              - "%Y-%m-%d_%H-%M"
            auto: false
            retention-policy: none
            retries: 1
    """))
    definition["replication-tasks"]["src"]["transport"] = transport
    definition["replication-tasks"]["src"]["properties"] = properties
    definition["replication-tasks"]["src"]["encryption"] = encryption

    if (properties and source_encrypted) or encryption:
        run_replication_test(definition)

        if encryption == "inherit":
            encryptionroot = "data/dst"
        else:
            encryptionroot = "data/dst/child/grandchild"

        assert subprocess.check_output(
            "zfs get -H -p encryptionroot data/dst/child/grandchild",
            encoding="utf-8", shell=True
        ).split("\n")[0].split("\t")[2] == encryptionroot
    else:
        error = run_replication_test(definition, success=False)

        if properties:
            assert error.error == (
                "Destination dataset 'data/dst/child/grandchild' must be encrypted (as one of its ancestors is "
                "encrypted). Refusing to transfer unencrypted source dataset 'data/src'. Please, set up replication "
                "task encryption in order to replicate this dataset."
            )
        else:
            assert error.error == (
                "Destination dataset 'data/dst/child/grandchild' must be encrypted (as one of its ancestors is "
                "encrypted). Refusing to transfer source dataset 'data/src' without properties and without replication "
                "task encryption."
            )
