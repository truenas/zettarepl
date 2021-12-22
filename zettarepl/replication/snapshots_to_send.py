# -*- coding=utf-8 -*-
import logging

from zettarepl.snapshot.name import parsed_snapshot_sort_key

logger = logging.getLogger(__name__)

__all__ = ["get_parsed_incremental_base"]


def get_parsed_incremental_base(parsed_src_snapshots, parsed_dst_snapshots):
    try:
        return sorted(
            set(parsed_src_snapshots) & set(parsed_dst_snapshots),
            key=parsed_snapshot_sort_key,
        )[-1]
    except IndexError:
        return None
