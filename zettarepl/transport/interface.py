# -*- coding=utf-8 -*-
import logging

logger = logging.getLogger(__name__)

__all__ = ["Shell", "ExecException", "Transport"]


class Shell:
    def exec(self, args, encoding="utf8"):
        raise NotImplementedError

    def put_file(self, f, dst_path):
        raise NotImplementedError


class ExecException(Exception):
    pass


class Transport:
    @classmethod
    def from_data(cls, data):
        raise NotImplementedError

    def __hash__(self):
        raise NotImplementedError

    def create_shell(self) -> Shell:
        raise NotImplementedError

    def push_snapshot(self, shell: Shell, source_dataset: str, target_dataset: str, snapshot: str, recursive: bool,
                      incremental_base: str, receive_resume_token: str):
        raise NotImplementedError
