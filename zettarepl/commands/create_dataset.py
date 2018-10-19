# -*- coding=utf-8 -*-
import logging

from zettarepl.dataset.create import create_dataset as create_dataset_on_shell

from .utils import get_transport

logger = logging.getLogger(__name__)

__all__ = ["create_dataset"]


def create_dataset(args):
    transport = get_transport(args.definition_path, args.transport)
    create_dataset_on_shell(transport.shell(transport), args.name)
