from abc import ABC
import json
import pandas as pd
import logging
from pathlib import  WindowsPath
log = logging.getLogger(__file__)


class Underlying(ABC):
    """
    Tradable object.
    Bars are stored in a pandas DataFrame: 
    - UTC dates in index
    - Columns are MultiIndex:
        - Level1 : value: the current value of the asset
            - Level2 : contains Currencies in which the level1 values are expressed
        - Level1 : gain : the daily pnl of the asset
            - Level2 : contains Currencies in which the level1 values are expressed
    """
    _instances = {}
    FOLDER = WindowsPath(r".\underlyings")

    def __init__(self, name: str, bars: pd.DataFrame = None, **kwargs) -> None:
        self.name = name
        if bars is None:
            self.bars = pd.DataFrame(index=pd.Index(
                [], name="date", dtype='datetime64[ns]'))
        else:
            self.bars = bars
        self.references = kwargs
        self._instances[name] = self

    @classmethod
    def exists(cls, name: str)-> bool:
        return cls.FOLDER.joinpath(name).exists()

    @classmethod
    def create(cls, name: str):
        return cls(name)

    def __str__(self) -> str:
        return f"{self.__class__.__name__}.{self.name}"

    def __repr__(self) -> str:
        return f'load("{self.name}")'

    @classmethod
    def load(cls, name: str):
        if name in cls._instances:
            return cls._instances[name]
        return cls._load(name)

    @classmethod
    def _load(cls, name: str):
        folder = cls.FOLDER.joinpath(name)
        bars_path = folder.joinpath("bars")
        ref_path = folder.joinpath("references")
        references = json.loads(ref_path.open('r').read())
        df = pd.read_csv(bars_path)
        df["date"] = pd.to_datetime(df.date)
        df = df.set_index("date")
        obj = cls(name=name, bars=df, **references)
        return obj
