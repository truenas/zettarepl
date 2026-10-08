# -*- coding=utf-8 -*-
import argparse
import logging

from zettarepl.daemon.service import Daemon

logger = logging.getLogger(__name__)

__all__ = ["run_daemon"]


def run_daemon(args: argparse.Namespace) -> None:
    Daemon(args.logging.default_level).run()
