import os
import zipfile
from typing import IO, Any, Tuple

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
            p(f"Processing {arg}... ", end="")
            err = self.__process_arg(arg, symbols, store)
            if isinstance(err, FileNotFoundError):
                p("file not found")
                return 1, None
            if err is not None:
                p()
                return 2, err
            p("done.")
        return None, None

    def __process_arg(self, arg: str, symbols: dict[str, Symbol], store: Store) -> str | Exception | None:
        try:
            with zipfile.ZipFile(arg, "r") as z:
                for filename in z.namelist():
                    sym, ok = self.__parse_filename(filename)
                    if not ok:
                        continue
                    if sym.lower() not in symbols:
                        continue
                    with z.open(filename, "r") as f:
                        err = self.__process_file(f, symbols[sym.lower()], store)
                        if err is not None:
                            return err
        except (OSError, zipfile.BadZipFile) as e:
            return e
        return None

    @staticmethod
    def __process_file(file: IO[bytes], symbol: Symbol, store: Store) -> str | Exception | None:
        df = pd.read_csv(file, dtype="str", header=None, usecols=range(6))
        df[0] = df[0].astype(int)
        start = int(symbol.start.timestamp()) if symbol.start is not None else 0
        df = df[df[0] >= start]
        df[0] = pd.to_datetime(df[0], unit="s", utc=True)
        if symbol.time is not None:
            df[0] = df[0].dt.tz_convert(tzinfo(symbol.time))
        for ts, gf in df.groupby(pd.Grouper(key=0, freq="MS")):
            err = store.put(Block(symbol.name, symbol.market, ts.date(), gf))  # type: ignore[attr-defined]
            if err is not None:
                return err
        return None

    @staticmethod
    def __parse_filename(filename: str) -> Tuple[str, bool]:
        if not filename.endswith("_1.csv"):
            return "", False
        return filename.removesuffix("_1.csv"), True


def getcmd(name: str) -> Tuple[Cmd | None, str | None]:
    match name:
        case "import":
            return Import(), None
        case _:
            return None, f"command {name} not found in module {__name__}"
