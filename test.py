from hutu import Hutu
from tushare_fetch import TushareFetch
import sys


class MyTest(Hutu):
    def a(self):
        self.last_update_time = 20190315
        print(self.last_update_time)

    def down_hsgt(self, trade_date):
        t = TushareFetch()
        t.test(trade_date)


trade_date = sys.argv[1]
test = MyTest()
test.down_hsgt(trade_date)
