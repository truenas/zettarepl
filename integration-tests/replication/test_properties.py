# -*- coding=utf-8 -*-
import subprocess
import textwrap

import pytest
import yaml

from zettarepl.utils.test import run_replication_test, transports


@pytest.mark.parametrize("transport", transports())
def test_properties_exclude(transport):
    subprocess.call("zfs destroy -r tank/src", shell=True)
    subprocess.call("zfs receive -A tank/dst", shell=True)
    subprocess.call("zfs destroy -r tank/dst", shell=True)

    subprocess.check_call("zfs create -o compression=gzip-1 -o mountpoint=/src tank/src", shell=True)
    subprocess.check_call("zfs snapshot -r tank/src@2019-11-08_15-00", shell=True)

    definition = yaml.safe_load(textwrap.dedent("""\
        timezone: "UTC"

        replication-tasks:
          src:
            direction: push
            source-dataset: tank/src
            target-dataset: tank/dst
            recursive: false
            properties: true
            properties-exclude:
            - mountpoint
            also-include-naming-schema:
              - "%Y-%m-%d_%H-%M"
            auto: false
            retention-policy: none
            retries: 1
    """))
    definition["replication-tasks"]["src"]["transport"] = transport

    run_replication_test(definition)

    assert (
        subprocess.check_output(
            "zfs get -H compression tank/dst",
            encoding="utf-8", shell=True
        ).split("\n")[0].split("\t")[2] ==
        "gzip-1"
    )
    assert (
        subprocess.check_output(
            "zfs get -H mountpoint tank/dst",
            encoding="utf-8", shell=True
        ).split("\n")[0].split("\t")[2] ==
        "/mnt/tank/dst"
    )


@pytest.mark.parametrize("transport", transports())
def test_properties_override(transport):
    subprocess.call("zfs destroy -r tank/src", shell=True)
    subprocess.call("zfs receive -A tank/dst", shell=True)
    subprocess.call("zfs destroy -r tank/dst", shell=True)

    subprocess.check_call("zfs create tank/src", shell=True)
    subprocess.check_call("zfs snapshot -r tank/src@2019-11-08_15-00", shell=True)

    definition = yaml.safe_load(textwrap.dedent("""\
        timezone: "UTC"

        replication-tasks:
          src:
            direction: push
            source-dataset: tank/src
            target-dataset: tank/dst
            recursive: false
            properties: true
            properties-override:
              compression: gzip-9
            also-include-naming-schema:
              - "%Y-%m-%d_%H-%M"
            auto: false
            retention-policy: none
            retries: 1
    """))
    definition["replication-tasks"]["src"]["transport"] = transport

    run_replication_test(definition)

    assert (
        subprocess.check_output(
            "zfs get -H compression tank/dst",
            encoding="utf-8", shell=True
        ).split("\n")[0].split("\t")[2] ==
        "gzip-9"
    )


@pytest.mark.parametrize("config", [
    {"properties-exclude": ["mountpoint"]},
    {"properties-override": {"canmount": "off"}},
])
def test_properties_exclude_override_does_not_break_volume(config):
    subprocess.call("zfs destroy -r tank/src", shell=True)
    subprocess.call("zfs receive -A tank/dst", shell=True)
    subprocess.call("zfs destroy -r tank/dst", shell=True)

    subprocess.check_call("zfs create tank/src", shell=True)
    subprocess.check_call("zfs create -V 1m tank/src/vol", shell=True)
    subprocess.check_call("zfs snapshot -r tank/src@2019-11-08_15-00", shell=True)

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
            also-include-naming-schema:
              - "%Y-%m-%d_%H-%M"
            auto: false
            retention-policy: none
            retries: 1
    """))
    definition["replication-tasks"]["src"].update(config)

    run_replication_test(definition)
