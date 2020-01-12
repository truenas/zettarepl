# -*- coding=utf-8 -*-
from collections import deque
import logging
import threading

from zettarepl.transport.zfscli import get_receive_resume_token

logger = logging.getLogger(__name__)

__all__ = ["ReplicationMonitor"]


class ReplicationMonitor:
    def __init__(self, shell, dataset, poll_interval=60.0, fail_on_repeat_count=60):
        self.shell = shell
        self.dataset = dataset
        self.poll_interval = poll_interval
        self.fail_on_repeat_count = fail_on_repeat_count

        self.stop_event = threading.Event()

    def run(self):
        receive_resume_tokens = deque([], self.fail_on_repeat_count)
        while not self.stop_event.wait(self.poll_interval):
            receive_resume_tokens.append(get_receive_resume_token(self.shell, self.dataset))
            token_count = len(receive_resume_tokens)
            unique_count = len(set(receive_resume_tokens))
            logger.debug(f"receive_resume_tokens: count=%d, unique=%d", token_count, unique_count)
            if token_count == self.fail_on_repeat_count and unique_count == 1:
                return False

        return True

    def stop(self):
        self.stop_event.set()
