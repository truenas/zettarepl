# -*- coding=utf-8 -*-
import logging
import os
import typing

logger = logging.getLogger(__name__)

__all__ = ["render_zcp"]

with open(os.path.join(os.path.dirname(__file__), "recursive_snapshot_exclude.lua")) as f:
    zcp_program = f.read()


def render_vars(buffer: typing.IO[bytes], dataset: str, snapshot_name: str, excluded_datasets: typing.Iterable):
    buffer.write(f'dataset = "{dataset}"\n'.encode("utf-8"))
    buffer.write(f'snapshot_name = "{snapshot_name}"\n'.encode("utf-8"))
    buffer.write('excluded_datasets = {'.encode("utf-8"))

    for excluded_dataset in excluded_datasets:
        buffer.write(f'"{excluded_dataset}", '.encode("utf-8"))

    buffer.write('}\n'.encode("utf-8"))


def render_zcp(buffer: typing.IO[bytes], dataset: str, snapshot_name: str, excluded_datasets: typing.Iterable):
    render_vars(buffer, dataset, snapshot_name, excluded_datasets)
    buffer.write(zcp_program.encode("utf-8"))
