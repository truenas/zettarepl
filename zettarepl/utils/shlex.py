# -*- coding=utf-8 -*-
import logging
import shlex

logger = logging.getLogger(__name__)

__all__ = ["implode", "pipe"]


def implode(args):
    return " ".join([shlex.quote(arg) for arg in args])


def pipe(*cmds):
    # We need to enable pipefail because sometimes `zfs recv` can exit with successful exit code while it has done
    # nothing.
    # But we can't just run `sh -o pipefail` because it's not present everywhere (e.g. it's not present in dash).

    # We'll generate some shell code and eval it. `eval` evaluates what's in the stdout,
    # so we'll redirect stdout to 3 in subshells and redirect 3 to stdout in parent shell.
    command = "exec 3>&1\n"

    # What's inside will print text like:
    #   pipestatus0=1
    #   pipestatus1=0
    #   pipestatus2=0
    # We'll eval that to get these variables in our scope
    command += "eval $(\n"

    # We'll print 'pipestatusX=Y' to 4, and we'll to pass it to eval through stdout
    # We'll redirect real stdout to 3, parent shell will print it back to stdout
    # We'll close fd 3 because we don't need it
    command += "exec 4>&1 >&3 3>&-\n"

    command += " | ".join([f"{{\n{implode(args)} 4>&-; echo \"pipestatus{i}=$?;\" >&4\n}}"
                           for i, args in enumerate(cmds)]) + "\n"

    # close eval
    command += ")\n"

    # Fail with exit code of the first failed command
    command += "\n".join([f"[ $pipestatus{i} -ne 0 ] && exit $pipestatus{i}" for i in range(len(cmds))]) + "\n"

    # No command failed
    command += "exit 0"

    return ["sh", "-c", command]
