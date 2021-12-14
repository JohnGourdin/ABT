import cProfile
from pstats import Stats, SortKey

from pathlib import WindowsPath
from Strategy import Strategy
import pandas as pd


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
            if self.position.empty and m not in self.orders.underlying:
                self.buy(m, 1, data.name)
            else:
                c = self.position.index[0]
                if m > c:
                    self.buy(m, 1, data.name)
                    self.sell(c, 1, data.name)

        return super().on_data(data)


def run_script():
    r = ECOROLL.create("ecoroll")
    r.update()
    r.bars.to_csv(r"..\data.csv")


if __name__ == '__main__':
    do_profiling = True
    if do_profiling:
        with cProfile.Profile() as pr:
            run_script()

        with open('profiling_stats.txt', 'w') as stream:
            stats = Stats(pr, stream=stream)
            stats.strip_dirs()
            stats.sort_stats('time')
            stats.dump_stats('.prof_stats')
            stats.print_stats()
    else:
        start_game()
