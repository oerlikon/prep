import os
from pathlib import Path
from typing import Any, Tuple

import pandas as pd

from common import Cmd, Symbol, p, tzinfo
from fs import Block, Store


class Import(Cmd):
    def run(self, *args: str, **kwargs: Any) -> Tuple[int | None, str | Exception | None]:
        symbols: dict[str, Symbol] | None = kwargs.get("symbols")
        if not symbols:
            return None, None
        path: str | os.PathLike[str] | None = kwargs.get("path")
        path = path if path is not None else ""
        store = Store(path)
        for arg in args:
            sym, err = self.__parse_arg(arg)
            if err is not None:
                return 1, err
            if sym.lower() not in symbols:
                p(f"Skipping {arg}")
                continue
            p(f"Processing {arg}... ", end="")
            err = self.__process_arg(arg, symbols[sym.lower()], store)
            if isinstance(err, FileNotFoundError):
                p("file not found")
                return 1, None
            if err is not None:
                p()
                return 2, err
            p("done.")
        return None, None

    @staticmethod
    def __parse_arg(arg: str) -> Tuple[str, str | Exception | None]:
        return Path(arg).stem, None

    @staticmethod
    def __process_arg(arg: str, symbol: Symbol, store: Store) -> str | Exception | None:
        try:
            df = pd.read_csv(arg, dtype="str", skiprows=1, header=None, usecols=range(5))
        except OSError as e:
            return e
        if symbol.time is None:
            df[0] = pd.to_datetime(df[0], format="%Y%m%d %H:%M", utc=True)
        else:
            df[0] = pd.to_datetime(df[0], format="%Y%m%d %H:%M")
            df[0] = df[0].dt.tz_localize(tzinfo(symbol.time))
        for date, gf in df.groupby(df[0].dt.date):
            err = store.put(Block(symbol.name, symbol.market, date, gf))
            if err is not None:
                return err
        return None


def getcmd(name: str) -> Tuple[Cmd | None, str | None]:
    match name:
        case "import":
            return Import(), None
        case _:
            return None, f"command {name} not found in module {__name__}"
