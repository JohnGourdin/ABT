from pathlib import WindowsPath
from ABT.Strategy import Strategy
import pandas as pd
import unittest


class ECOROLL(Strategy):
    def update(self):
        d = []
        for g in WindowsPath(r"./sources").glob(r"ECO*\data"):
            df = pd.read_csv(g)
            d.append(df.set_index(pd.to_datetime(df.date)).OPEN_INT.dropna())
            d[-1].name = g.parent.name
        self.data = pd.concat(d, axis=1)
        super().update()

    def on_data(self, data: pd.Series):
        if data.max() > 1:
            m = data.idxmax()
            if self.position.empty:
                self.buy(m, 1, data.name)
            else:
                c = self.position.index[0]
                if m > c:
                    self.buy(m, 1, data.name)
                    self.sell(c, 1, data.name)

        return super().on_data(data)


class TestScript(unittest.TestCase):
    def test_roll(self):
        r = ECOROLL.create("ecoroll")
        r.update()
        r.bars.to_csv(r"..\data.csv")
