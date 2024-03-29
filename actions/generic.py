from typing import Any, Tuple

from common import Cmd


class Status(Cmd):
    def run(self, *args: str, **kwargs: Any) -> Tuple[int | None, str | Exception | None]:
        return 1, "not implemented"


class GC(Cmd):
    def run(self, *args: str, **kwargs: Any) -> Tuple[int | None, str | Exception | None]:
        return 1, "not implemented"


def getcmd(name: str) -> Tuple[Cmd | None, str | None]:
    match name:
        case "st":
            return Status(), None
        case "gc":
            return GC(), None
        case _:
            return None, f"command {name} not found in module {__name__}"
