# -*- coding=utf-8 -*-
import logging
import os

import jsonschema
import jsonschema.validators
import yaml

logger = logging.getLogger(__name__)

__all__ = ["periodic_snapshot_task_validator", "replication_task_validator", "schedule_validator", "schema_validator"]


class LocalResolver(jsonschema.RefResolver):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.base_uri_head = os.path.split(self.base_uri)[0]

    def resolve_remote(self, uri):
        head, tail = os.path.split(uri)
        if head == self.base_uri_head:
            with open(os.path.join(os.path.dirname(__file__), os.path.splitext(tail)[0] + ".yaml")) as f:
                return yaml.safe_load(f)

        return super().resolve_remote(uri)


def create_validator(filename):
    with open(os.path.join(os.path.dirname(__file__), filename)) as f:
        schema = yaml.safe_load(f)

    validator_cls = jsonschema.validators.validator_for(schema)
    validator_cls.check_schema(schema)
    resolver = LocalResolver.from_schema(schema)
    validator = validator_cls(schema, resolver=resolver)
    return validator


periodic_snapshot_task_validator = create_validator("periodic-snapshot-task.schema.yaml")
replication_task_validator = create_validator("replication-task.schema.yaml")
schedule_validator = create_validator("schedule.schema.yaml")
schema_validator = create_validator("schema.yaml")
