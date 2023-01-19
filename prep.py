#!/usr/bin/env python3

import os
import signal
import sys

from typing import Any, Dict, List, Optional, Tuple, Union

import conf


def main(argv: List[str]) -> Tuple[Optional[int], Optional[Union[str, Exception]]]:
    if len(argv) == 1:
        p("Usage:  {} <path> [<command> [<args>]]".format(argv[0]))
        return None, None

    path = argv[1]
    prepfile = os.path.join(path, '.prep')

    if not os.path.exists(prepfile):
        return 1, 'no .prep file at path: {}'.format(path)

    err = conf.load_from(prepfile)
    if err is not None:
        return 1, err
    if not conf.symbols() and not conf.actions():
        return 1, "empty prepfile?"

    cmds: Dict[str, object] = {}

    cmd = argv[2] if len(argv) > 2 else None

    if cmd is None or cmd not in cmds:
        print(list(conf.symbols().keys()), conf.symbols())
        print(list(conf.actions().keys()), conf.actions())
        p('Available actions:')
        for action in conf.actions().values():
            p('\t' + action.name + ('\t({})'.format(action.using) if action.using else ''))
        return None, None

    return None, None


def p(*what: Any, **mods: Any) -> None:
    print(*what, **mods, file=sys.stderr, flush=True)


if __name__ == '__main__':
    try:
        signal.signal(signal.SIGPIPE, signal.SIG_DFL)
        ret, err = main(sys.argv)
        if err is not None:
            p("Error:", err)
        if ret:
            exit(ret)
    except KeyboardInterrupt:
        print()
    sys.stderr.close()
