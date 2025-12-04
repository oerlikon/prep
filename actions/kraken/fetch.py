from pathlib import Path

import dl
from common import Cmd, Symbol
from util import p

from .client import Client


class Fetch(Cmd):
    def run(self, *args, **kwargs) -> tuple[int | None, str | Exception | None]:
        symbols: dict[str, Symbol] | None = kwargs.get("symbols")
        if not symbols:
            return None, None
        if not args:
            return 1, "dl path?"

        self._path, self._client = Path(args[0]), Client()

        for symbol in symbols.values():
            assert symbol.market == "Kraken"

            err = self._fetch_symbol(symbol)
            if err is not None:
                return 2, err

        return None, None

    def _fetch_symbol(self, symbol: Symbol) -> str | Exception | None:
        assert symbol.start is not None

        p(f"Fetching {symbol.market}:{symbol.name}... ", end="")

        last_ts, last_id, err = dl.lasts(self._path, symbol)
        if err is not None:
            p()
            return err

        if not last_ts:
            last_ts = symbol.start.timestamp()

        for trades, err in self._client.fetch_trades(symbol.name, last_ts, last_id):
            if err is not None:
                p()
                return err
            err = dl.append(self._path, symbol, trades)
            if err is not None:
                p()
                return err

        p("done.")

        return None
