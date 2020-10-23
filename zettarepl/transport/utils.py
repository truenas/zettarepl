# -*- coding=utf-8 -*-
import hashlib
import logging
import os

from .encryption_context import EncryptionContext
from .interface import ReplicationProcess, Shell

logger = logging.getLogger(__name__)

__all__ = ["get_properties_override", "put_file"]


def get_properties_override(process: ReplicationProcess, encryption_context: EncryptionContext):
    properties_override = {}
    if encryption_context:
        properties_override.update(**encryption_context.enter())
    properties_override.update(process.properties_override)
    return properties_override


def put_file(name, shell: Shell):
    local_path = os.path.join(os.path.dirname(__file__), "..", name)
    with open(local_path, "rb") as f:
        md5 = hashlib.md5(f.read()).hexdigest()
        f.seek(0)

        remote_path = f"/tmp/zettarepl--{name.replace('/', '--')}--{md5}"
        if not shell.exists(remote_path):
            shell.put_file(f, remote_path)

    return remote_path
