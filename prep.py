#!/usr/bin/env python3

import os.path
import signal
import sys

import strictyaml as yaml

from typing import Any, List, Optional, Tuple, Union


def main(argv: List[str]) -> Tuple[Optional[int], Optional[Union[str, Exception]]]:
    if len(argv) == 1:
        p("Usage:  {} <path> <command> [<args>]".format(argv[0]))
        return None, None

    path = argv[1]
    prepfile = os.path.join(path, '.prep')

    if not os.path.exists(prepfile):
        return 2, 'not a prep path: {}'.format(path)

    conf, err = r(prepfile)
    if err is not None:
        return 1, err

    cmd = argv[2] if len(argv) > 2 else None

    if cmd is None:
        import pprint
        pprint.pprint(conf)
        return None, None

    return None, None


def r(filename: str) -> Tuple[Optional[object], Optional[Exception]]:
    try:
        with open(filename, "r") as f:
            obj = yaml.load(f.read()).data
    except TypeError as err:
        return None, err
    except OSError as err:
        return None, err
    except yaml.YAMLError as err:
        return None, err
    return obj, None


def p(*what: Any, **mods: Any) -> None:
    print(*what, **mods, file=sys.stderr, flush=True)


if __name__ == '__main__':
    try:
        signal.signal(signal.SIGPIPE, signal.SIG_DFL)
        ret, err = main(sys.argv)
        if err is not None:
            p("Error:", err)
        if ret != 0:
            exit(ret)
    except KeyboardInterrupt:
        print()
    sys.stderr.close()
