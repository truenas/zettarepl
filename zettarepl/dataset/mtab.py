# -*- coding=utf-8 -*-
import logging
import re

from zettarepl.transport.interface import Shell

logger = logging.getLogger(__name__)

__all__ = ["Mtab"]


class Mtab:
    def __init__(self, shell: Shell):
        self.shell = shell
        self.mtab = None

    def get(self, dataset):
        if self.mtab is None:
            self.mtab = {}
            for line in self.shell.exec(["mount"]).strip().split("\n"):
                m = re.match(r"(.+) on (.+) (\(zfs|type zfs)", line)
                if m:
                    self.mtab[m.group(1)] = m.group(2)

        return self.mtab.get(dataset)
