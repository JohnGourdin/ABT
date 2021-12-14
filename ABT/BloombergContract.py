from pathlib import Path, WindowsPath
import json
import pandas as pd

from Underlying import Underlying


def load_bloomberg(folder: Path):
    data_path = folder.joinpath("data")
    ref_path = folder.joinpath("references")
    references = json.loads(ref_path.open('r').read())
    df = pd.read_csv(data_path)
    idx = pd.to_datetime(
        df.date) + pd.to_timedelta(references["fut_trading_hrs"].split('-')[-1]+":00")
    prices = df.set_index(idx)[["PX_LAST", "PX_HIGH", "PX_LOW", "PX_OPEN", ]].mul(
        references['px_pos_mult_factor'])
    stakes = df.set_index(idx)[["VOLUME", "OPEN_INT"]]
    return pd.concat((prices, stakes), axis=1), references


class BloombergContract(Underlying):
    FOLDER = WindowsPath(r".\sources")

    @classmethod
    def _load(cls, name) -> "BloombergContract":
        df, references = load_bloomberg(cls.FOLDER.joinpath(name))
        value = df.PX_LAST
        gain = value.dropna().diff(1)
        df = pd.concat((value, gain), axis=1)
        df.columns = pd.MultiIndex.from_tuples(
            [("value", references["crncy"]), ("gain", references["crncy"])])
        return cls(name=name, bars=df, **references)



if __name__ == "__main__":
    b = BloombergContract.load("ECO_2023_11")
    print(b.bars)
