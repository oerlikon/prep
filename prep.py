#!/usr/bin/env python3

import importlib
import signal
import sys
from collections.abc import Sequence
from pathlib import Path

import conf
from common import Action
from util import p


def main(argv: Sequence[str]) -> tuple[int | None, str | Exception | None]:
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

    actions: dict[str, Action] = {name: bind(action) for name, action in conf.actions().items()}

    cmd = argv[2] if len(argv) > 2 else None

    if cmd is None:
        p("Available commands:")

        def s(action: Action) -> str:
            if action.using:
                return "\t" + action.name + f"\t({action.using})"
            return "\t" + action.name

        p("\n".join(s(action) for action in actions.values()))
        return None, None

    if cmd not in actions:
        p(f"Unknown command: {cmd}")
        return 1, None

    return actions[cmd].run(*argv[3:], path=path, symbols=conf.symbols())


def bind(action: Action) -> Action:
    def run(self: Action, *args, **kwargs) -> tuple[int | None, str | Exception | None]:
        mod_name = self.using if self.using else "generic"
        try:
            mod = importlib.import_module("actions." + mod_name)
        except (ImportError, ModuleNotFoundError) as e:
            return 2, e
        mod_cmd, err = mod.get_cmd(self.name)
        if err is not None:
            return 2, err
        return mod_cmd.run(*args, **kwargs)

    action.fn = run
    return action


if __name__ == "__main__":
    try:
        if sys.platform != "win32":
            signal.signal(signal.SIGPIPE, signal.SIG_DFL)
        ret, err = main(sys.argv)
        if err is not None:
            p("Error:", err)
        if ret is not None:
            sys.exit(ret)
    except KeyboardInterrupt:
        p()
