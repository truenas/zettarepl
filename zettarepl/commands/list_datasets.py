# -*- coding=utf-8 -*-
import logging

from zettarepl.dataset.list import list_datasets as list_datasets_on_shell

from .utils import get_transport

logger = logging.getLogger(__name__)

__all__ = ["list_datasets"]


def list_datasets(args):
    transport = get_transport(args.definition_path, args.transport)
    print("\n".join(list_datasets_on_shell(transport.shell(transport))))
