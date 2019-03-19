from hutu import Hutu


class MyTest(Hutu):

    def a(self):
        self.last_update_time = 20190315
        print(self.last_update_time)


m = MyTest()
m.a()