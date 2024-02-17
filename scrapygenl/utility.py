#!/usr/bin/python3

import re
import sys

from logging import Logger, getLogger, INFO, Formatter, StreamHandler, FileHandler
from typing import Iterable


__all__ = ("join_strs_w_comma_conj", "set_up_logging", "text2slug")


def text2slug(text: str) -> str:
    nonslug_chars_re = re.compile("[^a-z0-9_\n.]")
    text = text.lower()
    text = text.replace(" ", "_")
    text = nonslug_chars_re.subn("+", text)[0]
    return text


# This function borrowed wholesale from notifdler2.utility
#
# A factory function used to set up a Logger object just the way we want it
def set_up_logging(name: str) -> Logger:
    # Init's the Logger object, sets its threshold to INFO, and sets the format
    logger_obj: Logger = getLogger(name=name)
    logger_obj.setLevel(INFO)
    formatter: Formatter = Formatter(
        f"[%(asctime)s] %(levelname)s: [{name}] %(message)s"
    )

    # Instances 2 handlers, one for stdout and one for a file
    handlers: dict[str, StreamHandler | FileHandler] = dict()  # type: ignore[type-arg]
    handlers["stdout"] = StreamHandler(sys.stdout)
    handlers["file"] = FileHandler(f"{name}.log", "a")

    # Configs both handlers and attaches them
    key: str
    for key in ("stdout", "file"):
        handlers[key].setLevel(INFO)
        handlers[key].setFormatter(formatter)
        logger_obj.addHandler(handlers[key])

    return logger_obj


# This function borrowed wholesale from advgame.utils. Just takes a l
def join_strs_w_comma_conj(str_iter: Iterable[str], conjunction: str = "and") -> str:
    """
    This function automates the task of joining a sequence of strings with
    commas and a conjunction.

    >>> join_strs_w_comma_conj(['foo'], 'and')
    'foo'
    >>> join_strs_w_comma_conj(['foo', 'bar'], 'and')
    'foo and bar'
    >>> join_strs_w_comma_conj(['foo', 'bar', 'baz'], 'and')
    'foo, bar, and baz'

    :str_list:    The sequence of strings to join.
    :conjunction: The conjunction to use with sequences longer than 1
    element. Typical values include 'and' or 'or'.
    :return:      Returns a grammatical comma-separated list string.
    """
    str_list: list[str] = list(str_iter)
    if len(str_list) == 0:
        return ""
    elif len(str_list) == 1:
        return str_list[0]
    elif len(str_list) == 2:
        return f"{str_list[0]} {conjunction} {str_list[1]}"
    else:
        return ", ".join(str_list[:-1]) + f", {conjunction} " + str_list[-1]


