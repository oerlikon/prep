#!/usr/bin/env python3

import signal
import sys

import strictyaml as yaml  # type: ignore

from typing import Any, List, Optional, Tuple, Union


def main(argv: List[str]) -> Tuple[Optional[int], Optional[Union[str, Exception]]]:
    return None, None


def r(filename: str) -> Tuple[Optional[object], Optional[Exception]]:
    try:
        with open(filename, "r") as f:
            g = yaml.load(f)
    except OSError as err:
        return None, err
    except yaml.YAMLError as err:
        return None, err
    return g, None


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
