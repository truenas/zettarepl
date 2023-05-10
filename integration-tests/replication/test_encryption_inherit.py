# -*- coding=utf-8 -*-
import subprocess
import textwrap

import pytest
import yaml

from zettarepl.utils.test import create_dataset, run_replication_test, transports


def test_inherit_encryption_when_parent_is_not_encrypted():
    subprocess.call("zfs destroy -r data/src", shell=True)
    subprocess.call("zfs destroy -r data/dst", shell=True)

    create_dataset("data/src")
    subprocess.check_call("zfs snapshot -r data/src@2018-10-01_01-00", shell=True)

    create_dataset("data/dst")

    definition = yaml.safe_load(textwrap.dedent("""\
        timezone: "UTC"

        replication-tasks:
          src:
            direction: push
            transport:
                type: local
            source-dataset: data/src
            target-dataset: data/dst/child/grandchild
            recursive: false
            encryption: inherit
            also-include-naming-schema:
              - "%Y-%m-%d_%H-%M"
            auto: false
            retention-policy: none
            retries: 1
    """))

    error = run_replication_test(definition, success=False)
    assert error.error == (
        "Encryption inheritance requested for destination dataset 'data/dst/child/grandchild', but its existing parent "
        "is not encrypted."
    )
