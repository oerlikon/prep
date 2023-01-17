from __future__ import annotations

import yaml

from dataclasses import dataclass, field
from typing import List, Optional, Union


@dataclass
class Config:
    symbols: List[Symbol] = field(default_factory=list)
    actions: List[Action] = field(default_factory=list)

    def load_from(self, filename: str) -> Optional[Union[str, Exception]]:
        try:
            with open(filename, "r") as f:
                co = yaml.safe_load(f)
            if co is None:
                return None
        except OSError as err:
            return err
        except yaml.YAMLError as err:
            return err
        self.symbols = [Symbol()]
        self.actions = [Action()]
        return None


@dataclass
class Symbol:
    pass


@dataclass
class Action:
    pass


_conf: Optional[Config] = None


def load_from(filename: str) -> Optional[Union[str, Exception]]:
    global _conf
    if _conf is None:
        _conf = Config()
    return _conf.load_from(filename)


def symbols() -> List[Symbol]:
    global _conf
    if _conf is None:
        return []
    return _conf.symbols


def actions() -> List[Action]:
    global _conf
    if _conf is None:
        return []
    return _conf.actions
