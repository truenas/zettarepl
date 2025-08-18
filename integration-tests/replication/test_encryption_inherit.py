# -*- coding=utf-8 -*-
import subprocess
import textwrap

import yaml

from zettarepl.utils.test import create_dataset, run_replication_test


def test_inherit_encryption_when_parent_is_not_encrypted():
    subprocess.call("zfs destroy -r tank/src", shell=True)
    subprocess.call("zfs destroy -r tank/dst", shell=True)

    create_dataset("tank/src")
    subprocess.check_call("zfs snapshot -r tank/src@2018-10-01_01-00", shell=True)

    create_dataset("tank/dst")

    definition = yaml.safe_load(textwrap.dedent("""\
        timezone: "UTC"

        replication-tasks:
          src:
            direction: push
            transport:
                type: local
            source-dataset: tank/src
            target-dataset: tank/dst/child/grandchild
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
        "Encryption inheritance requested for destination dataset 'tank/dst/child/grandchild', but its existing parent "
        "is not encrypted."
    )


def test_inherit_encryption_when_parent_is_encrypted():
    subprocess.call("zfs destroy -r tank/src", shell=True)
    subprocess.call("zfs destroy -r tank/dst", shell=True)

    create_dataset("tank/src")
    subprocess.check_call("zfs snapshot -r tank/src@2018-10-01_01-00", shell=True)

    create_dataset("tank/dst")
    create_dataset("tank/dst/child", encrypted=True)

    definition = yaml.safe_load(textwrap.dedent("""\
        timezone: "UTC"

        replication-tasks:
          src:
            direction: push
            transport:
                type: local
            source-dataset: tank/src
            target-dataset: tank/dst/child/grandchild
            recursive: false
            encryption: inherit
            also-include-naming-schema:
              - "%Y-%m-%d_%H-%M"
            auto: false
            retention-policy: none
            retries: 1
    """))

    run_replication_test(definition)
