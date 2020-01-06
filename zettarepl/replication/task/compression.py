# -*- coding=utf-8 -*-
from collections import namedtuple
import logging

logger = logging.getLogger(__name__)

__all__ = ["ReplicationCompression", "replication_compressions"]

ReplicationCompression = namedtuple("ReplicationCompression", ["compress", "decompress"])

replication_compressions = {
    "pigz": ReplicationCompression(["pigz"], ["pigz", "-d"]),
    "plzip": ReplicationCompression(["plzip"], ["plzip", "-d"]),
    "lz4": ReplicationCompression(["lz4c"], ["lz4c", "-d"]),
    "xz": ReplicationCompression(["xz"], ["xzdec"]),
}
