TradeRecord = tuple[
    float,  # timestamp
    str | int | float,  # price
    str | int | float,  # buy volume
    str | int | float,  # sell volume
    str | int | float,  # market volume
    str | int | float,  # limit volume
    int,  # trade id
]


def wsname(name: str) -> tuple[str, Exception | None]:
    name = name.upper()
    for pref, subs in {"XBT": "BTC", "XDG": "DOGE"}.items():
        if name.startswith(pref):
            name = subs + name[len(pref) :]
            break
    for suff in ("USD", "EUR"):
        if name.endswith(suff):
            return name[: -len(suff)] + "/" + suff, None
    return "", ValueError(f"can't derive wsname from name {name!r}")
