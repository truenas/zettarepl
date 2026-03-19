# -*- coding=utf-8 -
from collections.abc import Callable
import jsonschema.exceptions
import logging
import sys
from typing import Any

import yaml

from zettarepl.definition.definition import Definition
from zettarepl.transport.create import create_transport
from zettarepl.transport.interface import Transport

logger = logging.getLogger(__name__)

__all__ = ["load_definition", "load_definition_raw", "get_transport"]


def load_definition(path: str) -> Definition:
    return process_definition(path, Definition.from_data)


def load_definition_raw(path: str) -> dict[str, Any]:
    def cb(data: dict[str, Any]) -> dict[str, Any]:
        Definition.validate(data)
        return data

    return process_definition(path, cb)


def process_definition[T](path: str, cb: Callable[[dict[str, Any]], T]) -> T:
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


def get_transport(definition_path: str, transport_name: str | None) -> Transport:
    definition = load_definition_raw(definition_path)

    if transport_name:
        try:
            transport = definition.get("transports", {})[transport_name]
        except KeyError:
            sys.stderr.write(f"Invalid transport {transport_name!r}\n")
            sys.exit(1)

        return create_transport(transport)
    else:
        return create_transport({"type": "local"})
