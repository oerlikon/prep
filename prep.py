#!/usr/bin/env python3

import importlib
import signal
import sys
from pathlib import Path
from typing import Any, Sequence, Tuple

import conf
from common import Action, Cmd, p


def main(argv: Sequence[str]) -> Tuple[int | None, str | Exception | None]:
    if len(argv) == 1:
        p(f"Usage:  {argv[0]} <path> [<command> [<args>]]")
        return None, None

    path = argv[1]

    prepfile = Path(path, ".prep")
    if not prepfile.exists():
        return 1, f"no .prep file at path: {path}"

    err = conf.load(prepfile)
    if err is not None:
        return 1, err
    if not conf.symbols() and not conf.actions():
        return 1, "empty prepfile?"

    cmd = argv[2] if len(argv) > 2 else None

    cmds: dict[str, Cmd] = {
        "st": ActionCmd(Action("st")),
        "gc": ActionCmd(Action("gc")),
    }
    cmds |= {name: ActionCmd(action) for name, action in conf.actions().items()}

    if cmd is None:
        p("Available commands:")
        p(list_cmds(cmds))
        return None, None

    if cmd not in cmds:
        p(f"Unknown command: {cmd}")
        return 1, None

    if isinstance(cmds[cmd], ActionCmd):
        return cmds[cmd].run(*argv[3:], path=path, symbols=conf.symbols())

    raise NotImplementedError


class ActionCmd:
    def __init__(self, action: Action):
        assert action is not None
        self._action = action

    @property
    def action(self) -> Action:
        return self._action

    def run(self, *args: str, **kwargs: Any) -> Tuple[int | None, str | Exception | None]:
        mod_name = self._action.using if self._action.using else "generic"
        try:
            mod = importlib.import_module("actions." + mod_name)
        except (ImportError, ModuleNotFoundError) as e:
            return 2, e
        mod_cmd, err = mod.getcmd(self._action.name)
        if err is not None:
            return 2, err
        ret, err = mod_cmd.run(*args, **kwargs)
        return ret, err


def list_cmds(cmds: dict[str, Cmd]) -> str:
    def s(name: str, cmd: Cmd) -> str:
        if isinstance(cmd, ActionCmd) and cmd.action.using:
            return "\t" + name + f"\t({cmd.action.using})"
        return "\t" + name

    return "\n".join(s(name, cmd) for name, cmd in cmds.items())


if __name__ == "__main__":
    try:
        if sys.platform != "win32":
            signal.signal(signal.SIGPIPE, signal.SIG_DFL)
        ret, err = main(sys.argv)
        if err is not None:
            p("Error:", err)
        if ret:
            exit(ret)
    except KeyboardInterrupt:
        p()
    sys.stderr.close()
