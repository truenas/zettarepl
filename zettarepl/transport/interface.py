# -*- coding=utf-8 -*-
import logging

from .shell.interface import Shell

logger = logging.getLogger(__name__)

__all__ = ["Transport"]


class Transport:
    def __hash__(self):
        raise NotImplementedError

    def create_shell(self) -> Shell:
        raise NotImplementedError

    def push_snapshot(self, shell: Shell, source_dataset: str, target_dataset: str, snapshot: str,
                      incremental_base: str, receive_resume_token: str):
        raise NotImplementedError
