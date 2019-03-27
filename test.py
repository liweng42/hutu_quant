from hutu import Hutu
from tushare_fetch import TushareFetch


class MyTest(Hutu):
    def a(self):
        self.last_update_time = 20190315
        print(self.last_update_time)

    def down_hsgt(self):
        t = TushareFetch()
        t.test()


test = MyTest()
test.down_hsgt()
