# -*- coding=utf-8 -*-
import re

__all__ = ["compile_name_regex"]


def compile_name_regex(name_regex: str) -> re.Pattern[str]:
    return re.compile(f"({name_regex})$")
