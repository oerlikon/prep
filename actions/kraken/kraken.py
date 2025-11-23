from common import Cmd

from .fetch import Fetch
from .import_ import Import


def get_cmd(name: str) -> tuple[Cmd | None, str | None]:
    match name:
        case "fetch":
            return Fetch(), None
        case "import":
            return Import(), None
        case _:
            return None, f"command {name} not found in module {__name__}"
