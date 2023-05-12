from dataclasses import dataclass
from typing import Any, Protocol, Tuple


@dataclass
class Symbol:
    name: str
    market: str | None = None
    time: str | None = None
    start: str | None = None


@dataclass
class Action:
    name: str
    using: str | None = None


class Cmd(Protocol):
    def run(self, *args: str, **kwargs: Any) -> Tuple[int | None, str | Exception | None]:
        ...
