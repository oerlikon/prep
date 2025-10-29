from __future__ import annotations

import datetime
import os
from collections.abc import Iterable, Iterator, Mapping
from dataclasses import replace

import yaml

from common import Action, Symbol, timezone

_symbols: dict[str, Symbol] = {}
_actions: dict[str, Action] = {}


def symbols() -> dict[str, Symbol]:
    return _symbols


def actions() -> dict[str, Action]:
    return _actions


class Error(Exception):
    def __init__(self, message: str):
        super().__init__("config: " + message)


def load(filename: str | os.PathLike[str]) -> Exception | None:
    try:
        with open(filename, "r") as f:
            co = yaml.safe_load(f)
    except OSError as err:
        return err
    except yaml.YAMLError as err:
        return err

    co = co if co is not None else {}

    symbols: dict[str, Symbol] = {}
    actions: dict[str, Action] = {}

    try:
        for symbol in _walk_symbols(co.get("symbols")):
            if symbol.name.lower() in symbols:
                raise Error(f"duplicate symbol: {symbol.name}")
            symbols[symbol.name.lower()] = symbol
        for action in _walk_actions(co.get("actions")):
            if action.name in actions:
                raise Error(f"duplicate action: {action.name}")
            actions[action.name] = action
    except Error as err:
        return err

    global _symbols, _actions
    _symbols, _actions = symbols, actions
    return None


def _walk_symbols(
    node: Iterable[Mapping[str, object]] | None,
    symbol: Symbol | None = None,
) -> Iterator[Symbol]:
    if node is None:
        return
    if symbol is None:
        symbol = Symbol("")
    for item in node:
        name, symbols = item.get("name"), item.get("symbols")
        if name is not None and symbols is not None:
            raise Error(f"both name and more symbols in same node with name: {name}")
        for k, v in item.items():
            match k:
                case "name" | "symbols":
                    pass
                case "market":
                    if type(v) != str:
                        raise TypeError
                    symbol.market = v
                case "time":
                    if type(v) != str:
                        raise TypeError
                    if timezone(v) is None:
                        raise Error(f"unknown time zone: {v}")
                    symbol.time = v
                case "start":
                    if type(v) == int:
                        symbol.start = datetime.datetime(v, 1, 1)
                    elif type(v) == str:
                        symbol.start = datetime.datetime.fromisoformat(v)
                    elif type(v) == datetime.date:
                        symbol.start = datetime.datetime.combine(v, datetime.time.min)
                    elif type(v) == datetime.datetime:
                        symbol.start = v
                    else:
                        raise TypeError
                case _:
                    raise Error(f"unexpected key: {k}")
        if symbol.start is not None and symbol.start.tzinfo is None:
            if symbol.time is not None:
                symbol.start = symbol.start.replace(tzinfo=timezone(symbol.time))
            else:
                symbol.start = symbol.start.replace(tzinfo=datetime.timezone.utc)
        if name is not None:
            if type(name) != str:
                raise TypeError
            yield replace(symbol, name=name)
        elif symbols is not None:
            if not isinstance(symbols, Iterable):
                raise TypeError
            yield from _walk_symbols(symbols, replace(symbol))


def _walk_actions(
    node: Iterable[Mapping[str, object]] | None,
    action: Action | None = None,
) -> Iterator[Action]:
    if node is None:
        return
    if action is None:
        action = Action("")
    for item in node:
        name, actions = item.get("name"), item.get("actions")
        if name is not None and actions is not None:
            raise Error(f"both name and more actions in same node with name: {name}")
        for k, v in item.items():
            match k:
                case "name" | "actions":
                    pass
                case "using":
                    if type(v) != str:
                        raise TypeError
                    action.using = v
                case _:
                    raise Error(f"unexpected key: {k}")
        if name is not None:
            if type(name) != str:
                raise TypeError
            yield replace(action, name=name)
        elif actions is not None:
            if not isinstance(actions, Iterable):
                raise TypeError
            yield from _walk_actions(actions, replace(action))
