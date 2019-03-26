import pandas as pd
import time
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
from pylab import *
import os
import stock_const as const
from tushare_fetch import TushareFetch
from process_stock_data import ProcessStockData
import utility
from emotion import EmotionIndex


class HutuTrade():
    'hutu_trade'

    def __init__(self):
        pass

    def run_only_once(self):
        """
        初始化下载形成origin文件，初始化处理计算各个指标形成process文件，初始化计算emotion指标形成emotion文件
        """
        t = TushareFetch()
        t.run_only_once()
        p = ProcessStockData()
        p.run_only_once()
        e = EmotionIndex()
        e.run_only_once()

    def run_daily_job(self):
        """
        每日下午4点半例行执行，1.下载日更新数据/2.处理数据
        """
        # 下载更新数据
        # t = TushareFetch()
        # updated = t.run_daily_job()
        # if updated:
        #     # 处理数据
        #     p = ProcessStockData()
        #     updated = p.run_daily_job()
        #     if updated:
        #         e = EmotionIndex()
        #         e.run_daily_job()
        t = TushareFetch()
        t.run_daily_job()
        p = ProcessStockData()
        p.run_daily_job()
        e = EmotionIndex()
        e.run_daily_job()

    def show_300_plot(self):
        """
        画出沪深300走势图
        """
        filename = os.path.join(const.origin_data_index_day_path, const.CODE_INDEX_300 + '.csv')
        stock_data = pd.read_csv(filename)
        stock_data['trade_date'] = pd.to_datetime(stock_data['trade_date'], format='%Y%m%d')
        # print(stock_data)
        stock_data.set_index('trade_date')
        stock_data.plot(x='trade_date', y='close')
        plt.show()

    def show_emotion_plot(self):
        """
        画出emotion走势图，叠加收盘价走势
        """
        # plt.rcParams['font.sans-serif']=['SimHei']#用来正常显示中文标签
        plt.rcParams['axes.unicode_minus'] = False  # 用来正常显示负号

        filename = os.path.join(const.emotion_index_data_root_path, 'emotion_index.csv')
        stock_data = pd.read_csv(filename)
        stock_data['trade_date'] = pd.to_datetime(stock_data['trade_date'], format='%Y%m%d')
        #     print(stock_data)
        stock_data.set_index('trade_date')

        fig, ax1 = plt.subplots()
        ax1.plot(stock_data['trade_date'], stock_data['close'], 'r', label='close')
        plt.legend(loc=2)

        ax2 = ax1.twinx()
        ax2.plot(stock_data['trade_date'], stock_data['v'], label='emotion_index')
        plt.legend(loc=1)

        ax1.set_xlabel('trade_date')
        ax1.set_ylabel('close')
        ax2.set_ylabel('emotion_index')
        plt.gcf().autofmt_xdate()

        plt.show()


if __name__ == '__main__':
    # hutu_trade = HutuTrade()
    # hutu_trade.run_only_once()
    # hutu_trade.run_daily_job()
    # hutu_trade.show_emotion_plot()
    # t = TushareFetch()
    # t.get_cal_start_date()

    # e = EmotionIndex()
    # e.run_only_once()
    t = TushareFetch()
    t.only_once_hsgt_data()
    