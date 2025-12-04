from common import Cmd, Symbol


class Test(Cmd):
    def run(self, *args, **kwargs) -> tuple[int | None, str | Exception | None]:
        symbols: dict[str, Symbol] | None = kwargs.get("symbols")
        if not symbols:
            return None, None
        path = kwargs.get("path", "")

        _ = path

        return None, None
