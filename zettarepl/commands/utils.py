# -*- coding=utf-8 -
import jsonschema.exceptions
import logging
import sys

import yaml

from zettarepl.definition.definition import Definition
from zettarepl.transport.create import create_transport

logger = logging.getLogger(__name__)

__all__ = ["load_definition", "load_definition_raw", "get_transport"]


def load_definition(path):
    return process_definition(path, Definition.from_data)


def load_definition_raw(path):
    def cb(data):
        Definition.validate(data)
        return data

    return process_definition(path, cb)


def process_definition(path, cb):
    try:
        data = yaml.safe_load(path)
    except yaml.YAMLError as e:
        sys.stderr.write(f"Definition syntax error: {e!s}\n")
        sys.exit(1)

    try:
        return cb(data)
    except yaml.YAMLError as e:
        sys.stderr.write(f"Definition syntax error: {e!s}\n")
        sys.exit(1)
    except jsonschema.exceptions.ValidationError as e:
        sys.stderr.write(f"Definition validation error: {e!s}\n")
        sys.exit(1)
    except ValueError as e:
        sys.stderr.write(f"{e!s}\n")
        sys.exit(1)


def get_transport(definition_path, transport):
    definition = load_definition_raw(definition_path)

    if transport:
        try:
            transport = definition.get("transports", {})[transport]
        except KeyError:
            sys.stderr.write(f"Invalid transport {transport!r}\n")
            sys.exit(1)

        return create_transport(transport)
    else:
        return create_transport({"type": "local"})
