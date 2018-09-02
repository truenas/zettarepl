# -*- coding=utf-8 -*-
import logging
import subprocess

from .interface import *

logger = logging.getLogger(__name__)

__all__ = ["LocalShell"]


class LocalShell(Shell):
    def exec(self, args, encoding="utf8"):
        logger.debug("Running %r", args)

        result = subprocess.run(args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, encoding=encoding)
        if result.returncode != 0:
            logger.debug("Error %r: %r", result.returncode, result.stdout)
            raise ExecException(result.returncode, result.stdout)

        logger.debug("Success: %r", result.stdout)
        return result.stdout
