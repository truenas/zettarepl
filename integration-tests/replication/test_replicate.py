# -*- coding=utf-8 -*-
import subprocess
import textwrap

import pytest
import yaml

from zettarepl.utils.test import run_replication_test


@pytest.mark.parametrize("snapshot_to_destroy,error_text", [
    # Fake incomplete `zfs send -R` by removing one of the children's most recent snapshots.
    (
        "data/dst/child2@2021-08-23_19-30",
        (
            f"Last full ZFS replication failed to transfer all the children of the snapshot data/src@2021-08-23_19-30. "
            "The snapshot data/dst/child2@2021-08-23_19-30 was not transferred. Please run "
            "`zfs destroy -r data/dst@2021-08-23_19-30` on the target system and run replication again."
        ),
    ),
    # Older child snapshots might have been removed by retention or manually, we should not care about them.
    ("data/dst/child2@2021-08-23_19-25", None),
])
@pytest.mark.parametrize("snapshot_match_options", [
    {"also-include-naming-schema": ["%Y-%m-%d_%H-%M"]},
    {"name-regex": ".+"},
])
@pytest.mark.parametrize("take_new_snapshot", [True, False])
def test_replicate(snapshot_to_destroy, error_text, snapshot_match_options, take_new_snapshot):
    subprocess.call("zfs destroy -r data/src", shell=True)
    subprocess.call("zfs receive -A data/dst", shell=True)
    subprocess.call("zfs destroy -r data/dst", shell=True)

    subprocess.check_call("zfs create data/src", shell=True)
    subprocess.check_call("zfs create data/src/child1", shell=True)
    subprocess.check_call("zfs create data/src/child2", shell=True)
    subprocess.check_call("zfs snapshot -r data/src@2021-08-23_19-25", shell=True)
    subprocess.check_call("zfs snapshot -r data/src@2021-08-23_19-30", shell=True)
    subprocess.check_call("zfs send -R data/src@2021-08-23_19-25 | zfs recv data/dst", shell=True)
    subprocess.check_call("zfs send -R -i data/src@2021-08-23_19-25 data/src@2021-08-23_19-30 | "
                          "zfs recv data/dst", shell=True)
    subprocess.check_call(f"zfs destroy {snapshot_to_destroy}", shell=True)

    if take_new_snapshot:
        subprocess.check_call(f"zfs snapshot -r data/src@2021-08-23_19-35", shell=True)

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
            replicate: true
            auto: false
            retention-policy: none
            retries: 1
    """))
    definition["replication-tasks"]["src"].update(snapshot_match_options)

    error = run_replication_test(definition, success=error_text is None)
    if error_text is not None:
        assert error.error == error_text
