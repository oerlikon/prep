from __future__ import annotations

import yaml

from dataclasses import dataclass, field, replace
from typing import Dict, Iterator, List, Mapping, Optional


@dataclass
class Config:
    symbols: Dict[str, Symbol] = field(default_factory=dict)
    actions: Dict[str, Action] = field(default_factory=dict)

    def load_from(self, filename: str) -> Optional[Exception]:
        try:
            with open(filename, "r") as f:
                co = yaml.safe_load(f)
                if co is None:
                    return None
        except OSError as err:
            return err
        except yaml.YAMLError as err:
            return err
        try:
            for symbol in Config.__walk_symbols(co.get('symbols')):
                if symbol.name in self.symbols:
                    raise Error('duplicate symbol: {}'.format(symbol.name))
                self.symbols[symbol.name] = symbol
            for action in Config.__walk_actions(co.get('actions')):
                if action.name in self.actions:
                    raise Error('duplicate action: {}'.format(action.name))
                self.actions[action.name] = action
        except Error as err:
            return err
        return None

    @staticmethod
    def __walk_symbols(node: Optional[List[Mapping[str, object]]],
                       symbol: Optional[Symbol] = None) -> Iterator[Symbol]:
        if node is None:
            return
        if symbol is None:
            symbol = Symbol('')
        for item in node:
            name, symbols = item.get('name'), item.get('symbols')
            if name is not None and symbols is not None:
                raise Error('both name and more symbols in same node with name: {}'.format(name))
            for k, v in item.items():
                if k == 'name' or k == 'symbols':
                    pass
                elif k == 'market':
                    if not isinstance(v, str):
                        raise TypeError
                    symbol.market = v
                elif k == 'time':
                    if not isinstance(v, str):
                        raise TypeError
                    symbol.time = v
                elif k == 'start':
                    if not isinstance(v, str):
                        raise TypeError
                    symbol.start = v
                else:
                    raise Error('unexpected key: {}'.format(k))
            if name is not None:
                yield replace(symbol, name=name)
            elif symbols is not None:
                if not isinstance(symbols, list):
                    raise TypeError
                yield from Config.__walk_symbols(symbols, replace(symbol))

    @staticmethod
    def __walk_actions(node: Optional[List[Mapping[str, object]]],
                       action: Optional[Action] = None) -> Iterator[Action]:
        if node is None:
            return
        if action is None:
            action = Action('')
        for item in node:
            name, actions = item.get('name'), item.get('actions')
            if name is not None and actions is not None:
                raise Error('both name and more actions in same node with name: {}'.format(name))
            for k, v in item.items():
                if k == 'name' or k == 'actions':
                    pass
                elif k == 'using':
                    if not isinstance(v, str):
                        raise TypeError
                    action.using = v
                else:
                    raise Error('unexpected key: {}'.format(k))
            if name is not None:
                yield replace(action, name=name)
            elif actions is not None:
                if not isinstance(actions, list):
                    raise TypeError
                yield from Config.__walk_actions(actions, replace(action))


@dataclass
class Symbol:
    name: str
    market: Optional[str] = None
    time: Optional[str] = None
    start: Optional[str] = None


@dataclass
class Action:
    name: str
    using: Optional[str] = None


class Error(Exception):

    def __init__(self, message: str):
        super().__init__('config: ' + message)


_conf: Optional[Config] = None


def load_from(filename: str) -> Optional[Exception]:
    global _conf
    if _conf is None:
        _conf = Config()
    return _conf.load_from(filename)


def symbols() -> Dict[str, Symbol]:
    global _conf
    if _conf is None:
        return {}
    return _conf.symbols


def actions() -> Dict[str, Action]:
    global _conf
    if _conf is None:
        return {}
    return _conf.actions
