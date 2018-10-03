# -*- coding=utf-8 -*-
import jsonschema.exceptions
import logging
import sys

import yaml

from zettarepl.definition.definition import Definition
from zettarepl.scheduler.clock import Clock
from zettarepl.scheduler.tz_clock import TzClock
from zettarepl.scheduler.scheduler import Scheduler
from zettarepl.transport.local import LocalShell
from zettarepl.zettarepl import Zettarepl

logger = logging.getLogger(__name__)

__all__ = ["run"]


def run(args):
    try:
        definition = Definition.from_data(yaml.load(args.definition_path))
    except yaml.YAMLError as e:
        sys.stderr.write(f"Definition syntax error: {e!s}\n")
        sys.exit(1)
    except jsonschema.exceptions.ValidationError as e:
        sys.stderr.write(f"Definition validation error: {e!s}\n")
        sys.exit(1)
    except ValueError as e:
        sys.stderr.write(f"{e!s}\n")
        sys.exit(1)

    clock = Clock(args.once)
    tz_clock = TzClock(definition.timezone, clock.now)

    scheduler = Scheduler(clock, tz_clock)
    local_shell = LocalShell()

    zettarepl = Zettarepl(scheduler, local_shell)
    zettarepl.set_tasks(definition.tasks)
    zettarepl.run()
