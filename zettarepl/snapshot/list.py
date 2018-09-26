# -*- coding=utf-8 -*-
from collections import defaultdict, OrderedDict
import logging

from zettarepl.dataset.relationship import is_child
from zettarepl.transport.interface import Shell

from .snapshot import Snapshot

logger = logging.getLogger(__name__)

__all__ = ["list_snapshots", "multilist_snapshots", "group_snapshots_by_datasets"]


def list_snapshots(shell: Shell, dataset: str, recursive: bool) -> [Snapshot]:
    args = ["zfs", "list", "-t", "snapshot", "-H", "-o", "name", "-s", "name"]
    if recursive:
        args.extend(["-r"])
    else:
        args.extend(["-d", "1"])
    args.append(dataset)
    return list(map(lambda s: Snapshot(*s.split("@")), filter(None, shell.exec(args).split("\n"))))


def multilist_snapshots(shell: Shell, queries: [(str, bool)]) -> [Snapshot]:
    snapshots = []
    for dataset, recursive in simplify_snapshot_list_queries(queries):
        snapshots.extend(list_snapshots(shell, dataset, recursive))
    return snapshots


def simplify_snapshot_list_queries(queries: [(str, bool)]) -> [(str, bool)]:
    simple = []
    for dataset, recursive in sorted(queries, key=lambda q: (q[0], 0 if q[1] else 1)):
        if recursive:
            queries_may_include_this = filter(lambda q: q[1], simple)
        else:
            queries_may_include_this = simple

        if not any(is_child(dataset, ds) if r else dataset == ds
                   for ds, r in queries_may_include_this):
            simple.append((dataset, recursive))

    return simple


def group_snapshots_by_datasets(snapshots: [Snapshot]) -> {str: [str]}:
    datasets = defaultdict(list)
    for snapshot in snapshots:
        datasets[snapshot.dataset].append(snapshot.name)
    return OrderedDict(sorted(datasets.items(), key=lambda t: t[0]))
