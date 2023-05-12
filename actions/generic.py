from typing import Any, Tuple, Type

from common import Cmd


class Status(Cmd):
    def run(self, *args: str, **kwargs: Any) -> Tuple[int | None, str | Exception | None]:
        print("generic status")
        return None, None


class GC(Cmd):
    def run(self, *args: str, **kwargs: Any) -> Tuple[int | None, str | Exception | None]:
        print("generic GC")
        return None, Exception("just kidding")


def getcmd(name: str) -> Tuple[Cmd | None, str | None]:
    match name:
        case "st":
            return Status(), None
        case "gc":
            return GC(), None
        case _:
            return None, f"command {name} not found in module {__name__}"
