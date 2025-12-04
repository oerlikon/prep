from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Protocol


@dataclass
class Symbol:
    name: str
    market: str | None = None
    time: str | None = None
    start: datetime | None = None


class Cmd(Protocol):
    def run(self, *args, **kwargs) -> tuple[int | None, str | Exception | None]: ...


@dataclass
class Action(Cmd):
    name: str
    using: str | None = None
    fn: Callable[..., tuple[int | None, str | Exception | None]] | None = None

    def run(self, *args, **kwargs) -> tuple[int | None, str | Exception | None]:
        if self.fn is not None:
            return self.fn(self, *args, **kwargs)
        return None, None
