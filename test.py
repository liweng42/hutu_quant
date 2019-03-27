from hutu import Hutu
from tushare_fetch import TushareFetch

class MyTest(Hutu):
    def a(self):
        self.last_update_time = 20190315
        print(self.last_update_time)

    def down_hsgt(self):
        new_data = self.pro.query('moneyflow_hsgt', trade_date=trade_date)
        print(new_data)


t = TushareFetch()
t.test()
