# -*- coding=utf-8 -*-
import argparse
import logging

logger = logging.getLogger(__name__)

__all__ = ["run_daemon"]


def run_daemon(args: argparse.Namespace) -> None:
    from zettarepl.daemon.service import Daemon

    Daemon(args.logging.default_level).run()
