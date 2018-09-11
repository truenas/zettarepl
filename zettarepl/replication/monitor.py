# -*- coding=utf-8 -*-
from collections import deque
import logging
import threading

from zettarepl.transport.zfscli import get_receive_resume_token

logger = logging.getLogger(__name__)

__all__ = ["ReplicationMonitor"]


class ReplicationMonitor:
    def __init__(self, shell, dataset, poll_interval=60.0, fail_on_repeat_count=5):
        self.shell = shell
        self.dataset = dataset
        self.poll_interval = poll_interval
        self.fail_on_repeat_count = fail_on_repeat_count

        self.stop_event = threading.Event()

    def run(self):
        receive_resume_tokens = deque([], self.fail_on_repeat_count)
        while not self.stop_event.wait(self.poll_interval):
            receive_resume_tokens.append(get_receive_resume_token(self.shell, self.dataset))
            logger.debug(f"receive_resume_tokens: %r", receive_resume_tokens)
            if len(receive_resume_tokens) == self.fail_on_repeat_count and len(set(receive_resume_tokens)) == 1:
                return False

        return True

    def stop(self):
        self.stop_event.set()
