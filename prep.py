#!/usr/bin/env python3

import os
import signal
import sys

from typing import Any, List, Optional, Tuple, Union

import conf


def main(argv: List[str]) -> Tuple[Optional[int], Optional[Union[str, Exception]]]:
    if len(argv) == 1:
        p("Usage:  {} <path> <action> [<args>]".format(argv[0]))
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

    action = argv[2] if len(argv) > 2 else None

    if action is None:
        import pprint
        pprint.pprint(conf.symbols())
        pprint.pprint(conf.actions())
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
