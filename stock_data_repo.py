import stock_const as const
import os
import pandas as pd
import zlib, pickle
import redis


class StockDataRepo:
    '股票数据访问类，股票数据的存取都用这个类来完成，直接访问redis'

    def __init__(self):
        redis_host = "127.0.0.1"
        # redis_password = "test321"
        self.redis_cli = redis.StrictRedis(host=redis_host, port=6379, db=0)
        # self.redis_cli = redis.Redis(host='127.0.0.1', port=6379)

    def get_process_data_market_day_data(self, ts_code):
        """
        获取股票日K线数据
        """
        if not self.redis_cli.exists(ts_code):
            filename = os.path.join(const.process_data_market_day_path, ts_code + '.csv')
            if os.path.exists(filename):
                df = pd.read_csv(filename)
                self.redis_cli.set(ts_code, df.to_msgpack(compress='zlib'))
                return pd.read_msgpack(self.redis_cli.get(ts_code))
            else:
                return None
        else:
            return pd.read_msgpack(self.redis_cli.get(ts_code))