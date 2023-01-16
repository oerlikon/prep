from __future__ import annotations

import yaml

from dataclasses import dataclass, field
from typing import List, Optional, Tuple, Union


@dataclass
class Config:
    symbols: List[Symbol] = field(default_factory=list)
    actions: List[Action] = field(default_factory=list)


@dataclass
class Symbol:
    pass


@dataclass
class Action:
    pass


def load_from(filename: str) -> Tuple[Optional[Config], Optional[Union[str, Exception]]]:
    try:
        with open(filename, "r") as f:
            co = yaml.safe_load(f)
        if co is None:
            return None, None
        for k, v in co.items():
            print(k, v)
    except OSError as err:
        return None, err
    except yaml.YAMLError as err:
        return None, err
    return Config(), None
