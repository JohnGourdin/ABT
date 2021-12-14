
from datetime import datetime
import json
import pandas as pd
from BloombergContract import BloombergContract
from Underlying import Underlying


class Strategy(Underlying):
    """Strategy class"""

    def __init__(self, name: str, bars=None, data=pd.DataFrame([]),
                 orders=pd.DataFrame([], columns=['order_date', 'underlying', 'lots']),
                 transactions=pd.DataFrame([], index=pd.Index([], name="date", dtype='datetime64[ns]'), columns=['order_date', 'underlying', 'lots']),
                 position=pd.Series([], index=pd.Index([], name='underlying', dtype='O'), name='lots'), **kwargs) -> None:
        super().__init__(name, bars=bars, **kwargs)
        self.data = data
        self.transactions = transactions

        self.orders = orders
        self.position = position

    def on_bar(self, bar: pd.Series):
        pass

    def on_data(self, data: pd.Series):
        pass

    def get_data(self):
        raise NotImplementedError()

    def update(self):
        data_generator = (i for _, i in self.data.iterrows())

        if self.bars.empty:
            data = next(data_generator)
            self.on_data(data)
            bar = pd.Series(name=data.name)
            self.on_bar(bar)

        for data in data_generator:
            self.on_data(data)
            self.fill_bars_until(data.name)

    def get_position(self, as_of=None):
        if as_of is None:
            return self.transactions.lots.unstack("underlying").sum(0)
        else:
            return self.transactions.loc[:as_of].lots.unstack("underlying").sum(0)

    @property
    def get_current_date(self):
        return max(self.bars.index.max(), self.data.index.max())

    def fill_bars_until(self, date):
        if pd.isnull(date):
            print(date)
        if self.orders.empty and self.position.empty:
            return
        if self.bars.empty:
            last_bar_date = pd.to_datetime("1980-01-01")
        else:
            last_bar_date = self.bars.iloc[-1].name

        if last_bar_date >= date:
            return

        if not self.orders.empty:
            orders = self.orders
            self.orders = pd.DataFrame([], columns=['order_date', 'underlying', 'lots'])
            ous = [load(row.underlying).bars.loc[last_bar_date+pd.to_timedelta("1 ns"):date] for _, row in orders.iterrows()]
            dt = min([i.index.min() for i in ous])
            self.fill_bars_until(dt)

            left_over_orders = pd.DataFrame([])
            for data, row in zip(ous, orders.iterrows()):
                row = row[1]
                if data.loc[dt+pd.to_timedelta("1 ns"):].empty:
                    left_over_orders = left_over_orders.append(row)
                else:
                    self.transactions = self.transactions.append(
                        pd.Series(row, name=data.loc[dt+pd.to_timedelta("1 ns"):].index[0]))
                    self.position = self.position.add(
                        pd.Series([row.lots], index=[row.underlying]), fill_value=0)
            self.fill_bars_until(date)
            self.orders = left_over_orders

        else:
            summed_bars = None
            for name, lots in self.position.items():
                a = load(
                    name).bars.loc[last_bar_date+pd.to_timedelta("1 ns"):date, ['value', 'gain']].mul(lots)
                if summed_bars is not None:
                    summed_bars.add(a, fill_value=0)
                else:
                    summed_bars = a

            for _, bar in summed_bars.iterrows():
                self.on_bar(bar)
                self.bars = pd.concat((self.bars, pd.DataFrame(bar).transpose()), axis=0)

    def buy(self, underlying_name: str, lots: float, date):
        if lots == 0:
            return
        self.orders = self.orders.append(
            pd.Series({"order_date": date, "underlying": underlying_name, "lots": lots}), ignore_index=True)

    def sell(self, underlying_name: str, lots: float, date):
        return self.buy(underlying_name, -lots, date)

    def set_position_to(self, underlying_name: str, lots: float, date):
        position = self.transactions.loc[pd.IndexSlice[:date,
                                                       underlying_name], "lots"].sum()
        position += self.orders[self.orders.underlying ==
                                underlying_name].lots.sum()

        self.buy(underlying_name,  lots-position, date)

    @classmethod
    def _load(cls, name: str) -> Underlying:
        obj = super()._load(name)
        data_path = cls.FOLDER.joinpath(name).joinpath("data")
        orders_path = cls.FOLDER.joinpath(name).joinpath("orders")

        data = pd.read_csv(data_path)
        orders = pd.read_csv(orders_path)

        data["date"] = pd.to_datetime(data.date)
        obj.data = data.set_index("date")

        orders["date"] = pd.to_datetime(orders.date)
        obj.orders = orders.set_index("date")
        return obj


def load(name, cls=None):
    if cls is None:
        for cls in [Underlying, BloombergContract]:
            if cls.exists(name):
                break
    return cls.load(name)
