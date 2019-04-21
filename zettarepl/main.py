# -*- coding=utf-8 -*-
import argparse
import logging
import sys

import coloredlogs

from .commands.create_dataset import create_dataset
from .commands.list_datasets import list_datasets
from .commands.run import run
from .utils.logging import LongStringsFilter, ReplicationTaskLoggingLevelFilter

logger = logging.getLogger(__name__)


class LoggingConfiguration:
    def __init__(self, value):
        self.default_level = logging.INFO
        self.loggers = []

        for v in value.split(","):
            if ":" in v:
                logger_name, level_name = v.split(":", 1)
                try:
                    level = logging._nameToLevel[level_name.upper()]
                except KeyError:
                    raise argparse.ArgumentTypeError(f"Unknown logging level: {level_name!r}")

                self.loggers.append((logger_name, level))
            else:
                level_name = v
                try:
                    level = logging._nameToLevel[level_name.upper()]
                except KeyError:
                    raise argparse.ArgumentTypeError(f"Unknown logging level: {level_name!r}")

                self.default_level = level


def main():
    parser = argparse.ArgumentParser(prog="zettarepl")

    parser.add_argument("-l", "--logging", type=LoggingConfiguration, default="info",
                        help='Per-logger logging level configuration. E.g.: "info", "warning" or "debug,paramiko:info"')

    subparsers = parser.add_subparsers()
    subparsers.required = True
    subparsers.dest = "command"

    list_datasets_parser = subparsers.add_parser("list_datasets", help="List datasets")
    list_datasets_parser.add_argument("definition_path", type=argparse.FileType("r"))
    list_datasets_parser.add_argument("transport", nargs="?")
    list_datasets_parser.set_defaults(func=list_datasets)

    run_parser = subparsers.add_parser("create_dataset", help="Create dataset")
    run_parser.add_argument("definition_path", type=argparse.FileType("r"))
    run_parser.add_argument("name")
    run_parser.add_argument("transport", nargs="?")
    run_parser.set_defaults(func=create_dataset)

    run_parser = subparsers.add_parser("run", help="Continuously run scheduled replication tasks")
    run_parser.add_argument("definition_path", type=argparse.FileType("r"))
    run_parser.add_argument("--once", action="store_true",
                            help="Run replication tasks scheduled for current moment of time and exit")
    run_parser.set_defaults(func=run)

    args = parser.parse_args()

    logging_format = "[%(asctime)s] %(levelname)-8s [%(threadName)s] [%(name)s] %(message)s"
    logging.basicConfig(level=args.logging.default_level, format=logging_format)
    if sys.stdout.isatty():
        coloredlogs.install(level=args.logging.default_level, fmt=logging_format)
    for name, level in args.logging.loggers:
        logging.getLogger(name).setLevel(level)
    for handler in logging.getLogger().handlers:
        handler.addFilter(LongStringsFilter())
        handler.addFilter(ReplicationTaskLoggingLevelFilter())

    args.func(args)
