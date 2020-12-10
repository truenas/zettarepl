# -*- coding=utf-8 -*-
import logging

from zettarepl.zettarepl import create_zettarepl

from .utils import load_definition

logger = logging.getLogger(__name__)

__all__ = ["run"]


def run(args):
    definition = load_definition(args.definition_path)

    zettarepl = create_zettarepl(definition, clock_args=(args.once,))
    zettarepl.set_tasks(definition.tasks)
    zettarepl.run()
