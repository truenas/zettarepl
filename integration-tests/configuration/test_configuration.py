# -*- coding=utf-8 -*-
import textwrap

import pytest
import yaml

from zettarepl.definition.definition import Definition, DefinitionErrors


@pytest.mark.parametrize("definition,error", [
    (
        """\
            timezone: "UTC"

            periodic-snapshot-tasks:
              src-files-1w:
                dataset: data/src/files
                recursive: true
                lifetime: P7D
                naming-schema: "auto-%Y%m%d.%H%M-1w"
                schedule:
                  minute: "0"
                  hour: "*"
                  begin: "00:00"
                  end: "23:45"

              src:
                dataset: data/src
                recursive: true
                lifetime: P7D
                naming-schema: "auto-backup-%Y-%m-%d_%H-%M-1w"
                schedule:
                  minute: "0"
                  hour: "0"
                  begin: "00:00"
                  end: "23:59"

            replication-tasks:
              src:
                direction: push
                transport:
                  type: local
                source-dataset: data/src
                target-dataset: data/dst
                recursive: true
                properties: true
                replicate: true
                periodic-snapshot-tasks:
                  - src
                  - src-files-1w
                auto: true
                schedule:
                  minute: "0"
                  hour: "0"
                retention-policy: source
                hold-pending-snapshots: true
        """,
        "When parsing replication task src: Replication tasks that replicate the entire filesystem can only use "
        "periodic snapshot tasks that take recursive snapshots of the dataset being replicated (or its ancestor). "
        "Snapshot task 'src-files-1w' violates this requirement."
    ),
    (
        """\
            timezone: "UTC"

            periodic-snapshot-tasks:
              src-files-1w:
                dataset: data/src/files
                recursive: true
                lifetime: P7D
                naming-schema: "auto-%Y%m%d.%H%M-1w"
                schedule:
                  minute: "0"
                  hour: "*"
                  begin: "00:00"
                  end: "23:45"

              src:
                dataset: data/src
                recursive: true
                lifetime: P7D
                naming-schema: "auto-backup-%Y-%m-%d_%H-%M-1w"
                schedule:
                  minute: "0"
                  hour: "0"
                  begin: "00:00"
                  end: "23:59"

            replication-tasks:
              src:
                direction: push
                transport:
                  type: local
                source-dataset: [data/src, data/src/files]
                target-dataset: data/dst
                recursive: true
                properties: true
                replicate: true
                periodic-snapshot-tasks:
                  - src
                  - src-files-1w
                auto: true
                schedule:
                  minute: "0"
                  hour: "0"
                retention-policy: source
                hold-pending-snapshots: true
        """,
        "When parsing replication task src: Replication task that replicates the entire filesystem can't replicate "
        "both 'data/src' and its child 'data/src/files'",
    ),
])
def test_configuration_error(definition, error):
    with pytest.raises(DefinitionErrors) as ve:
        Definition.from_data(yaml.safe_load(textwrap.dedent(definition)))

    assert str(ve.value) == error
