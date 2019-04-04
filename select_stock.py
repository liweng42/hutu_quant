from hutu import Hutu
import pandas as pd
import numpy as np
import time
from datetime import datetime
import os
import stock_const as const
import utility
from log_manager import logger


class SelectStock(Hutu):
    '筛选股票'

    def __init__(self):
        super(SelectStock, self).__init__()
        self.check_folder()
        # 这里初始化上次更新日期为 emotion 目录下的上证指数文件最后更新日期
        self.last_update_time = self.get_last_update_time(
            self.hutu_type['emotion'])

    def check_folder(self):
        """
        检测目录结构，不存在就创建
        """
        # select_data 目录
        if not os.path.exists(const.select_data_root_path):
            os.makedirs(const.select_data_root_path)
            print('mkdir %s' % const.select_data_root_path)
        logger.debug('select_data 目录检测完毕！')

    def select_rise_limit_times(self, duration_days=31, times=5):
        """
        找出过去duration_days天内连板数大于等于times次的股票
        """
        logger.info('\n=====找出过去duration_days天内连板数大于等于times次的股票=====')
        logger.info('开始时间：%s' % datetime.now())
        if not self.debug:
            stock_list = pd.read_csv(const.ORIGIN_DATA_STOCK_BASIC)
        else:
            stock_list = pd.read_csv(const.DEBUG_DATA_STOCK_BASIC)
        count = 1
        df = pd.DataFrame(columns=['ts_code'])
        # print(df)
        p_filename = 'select_rise_limit_times_%s_%s.csv' % (duration_days,
                                                            times)
        p_filename = os.path.join(const.select_data_root_path, p_filename)
        # print(p_filename)
        for index, row in stock_list.iterrows():
            o_filename = os.path.join(const.process_data_market_day_path,
                                      row["ts_code"] + '.csv')
            code = row["ts_code"]
            length = len(stock_list)
            if os.path.exists(o_filename):
                stock_data = pd.read_csv(o_filename)
                # 取近30天的数据
                stock_data = stock_data[0:duration_days]
                # logger.debug(stock_data)
                tmp = np.where(stock_data['rise_limit_count'] >= times)
                # logger.debug(code)
                # logger.debug(tmp)
                # logger.debug(len(tmp[0]))
                if (len(tmp[0]) > 0):
                    df = df.append(
                        pd.Series([code], index=['ts_code']),
                        ignore_index=True)
                    # print(df)
                # logger.debug(len(tmp[0]))
            percent = round(1.00 * count / length * 100, 2)
            print(
                '进度 : %s [%d/%d]，code:%s' % (
                    (str(percent) + '%', count, length, code)),
                end='\r')

            count = count + 1
        # print(df)
        df.to_csv(p_filename, index=False)
        logger.info('结束时间：%s' % datetime.now())
        logger.info('=====找出过去duration_days天内连板数大于等于times次的股票 done!=====')
