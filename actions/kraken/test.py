from datetime import datetime, timezone
from pathlib import Path

import dl
from common import Cmd, Symbol
from util import tss


class Test(Cmd):
    def run(self, *args, **kwargs) -> tuple[int | None, str | Exception | None]:
        symbols: dict[str, Symbol] | None = kwargs.get("symbols")
        if not symbols:
            return None, None

        path = Path(args[0]) if len(args) > 0 else None
        assert path is not None

        recs, err = dl.tails(path, symbols["xbtusd"], datetime(2025, 12, 1, tzinfo=timezone.utc))
        if err is not None:
            return 2, err

        print("...", len(recs))
        for rec in recs[:22]:
            print(tss(rec.ts))

        return None, None
