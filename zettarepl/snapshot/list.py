# -*- coding=utf-8 -*-
from collections import defaultdict, OrderedDict
import logging

from zettarepl.transport.interface import Shell

from .snapshot import Snapshot

logger = logging.getLogger(__name__)

__all__ = ["list_snapshots", "group_snapshots_by_datasets"]


def list_snapshots(shell: Shell, dataset: str, recursive: bool) -> [Snapshot]:
    args = ["zfs", "list", "-t", "snapshot", "-H", "-o", "name", "-s", "name"]
    if recursive:
        args.extend(["-r"])
    else:
        args.extend(["-d", "1"])
    args.append(dataset)
    return list(map(lambda s: Snapshot(*s.split("@")), filter(None, shell.exec(args).split("\n"))))


def group_snapshots_by_datasets(snapshots: [Snapshot]) -> {str: [str]}:
    datasets = defaultdict(list)
    for snapshot in snapshots:
        datasets[snapshot.dataset].append(snapshot.name)
    return OrderedDict(sorted(datasets.items(), key=lambda t: t[0]))
