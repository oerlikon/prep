import os
from datetime import datetime, timezone
from pathlib import Path

from common import Cmd, Symbol
from util import p, parse_ts, ts, zx

from .client import Client


class Fetch(Cmd):
    def run(self, *args, **kwargs) -> tuple[int | None, str | Exception | None]:
        symbols: dict[str, Symbol] | None = kwargs.get("symbols")
        if not symbols:
            return None, None
        if not args:
            return 1, "dl path?"

        self._dl, self._client = Path(args[0]), Client()

        for symbol in symbols.values():
            assert symbol.market == "Kraken"

            err = self._fetch_symbol(symbol)
            if err is not None:
                return 2, err

        return None, None

    def _fetch_symbol(self, symbol: Symbol) -> str | Exception | None:
        assert symbol.start is not None

        p(f"Fetching {symbol.market}:{symbol.name}... ", end="")

        try:
            outpath = self._dl / f"kraken.{symbol.name.lower()}.trades.csv"
            if outpath.exists():
                start, last_id, err = self._parse_last_record(outpath)
                if err is not None:
                    p()
                    return err
                if start == 0:
                    start = symbol.start.timestamp()
            else:
                self._dl.mkdir(mode=0o755, parents=True, exist_ok=True)
                with open(outpath, "w") as f:
                    pass
                os.chmod(outpath, 0o644)
                start, last_id = symbol.start.timestamp(), 0

            with open(outpath, "a") as outfile:
                for trades, err in self._client._fetch_trades(symbol.name.upper(), start, last_id):
                    if err is not None:
                        p()
                        return err
                    for trade in trades:
                        outfile.write(
                            ",".join(
                                [ts(datetime.fromtimestamp(trade[0], tz=timezone.utc))]
                                + [zx(s) for s in trade[1:-1]]
                                + [str(trade[-1])]
                            )
                            + "\n"
                        )
                    outfile.flush()

        except OSError as err:
            p()
            return err

        p("done.")

        return None

    @staticmethod
    def _parse_last_record(path: str | os.PathLike[str]) -> tuple[float, int, Exception | None]:
        with open(path, "rb") as f:
            f.seek(0, 2)
            size = f.tell()
            if size == 0:
                return 0, 0, None
            start = max(0, size - 4096)
            f.seek(start)
            lines = f.read(size - start).splitlines()
            if not lines:
                return 0, 0, None
            for b in reversed(lines):
                b = b.strip()
                if not b:
                    continue
                fields = b.decode("utf-8", errors="replace").split(",")
                if len(fields) < 7:
                    return 0, 0, ValueError(f"unexpected {fields}")
                try:
                    dt, last_id = parse_ts(fields[0]), int(fields[6])
                except ValueError as err:
                    return 0, 0, err
                return dt.timestamp(), last_id, None
        return 0, 0, None
