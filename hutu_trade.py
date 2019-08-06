import pandas as pd
from pandas.plotting import register_matplotlib_converters
register_matplotlib_converters()
import matplotlib.pyplot as plt
import os
import stock_const as const
from tushare_fetch import TushareFetch
from process_stock_data import ProcessStockData
from emotion import EmotionIndex
from select_stock import SelectStock
import sys
from log_manager import logger


class HutuTrade():
    'hutu_trade'

    def __init__(self):
        pass

    def run_only_once(self):
        """
        初始化下载形成origin文件，初始化处理计算各个指标形成process文件，初始化计算emotion指标形成emotion文件
        """
        logger.info('run_only_once')
        t = TushareFetch()
        t.run_only_once()
        p = ProcessStockData()
        p.run_only_once()
        e = EmotionIndex()
        e.run_only_once()

    def run_daily_job(self):
        """
        每日下午6点例行执行，1.下载日更新数据/2.处理数据
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
        logger.info('run_daily_job start')
        logger.info('TushareFetch run_daily_job start')
        t = TushareFetch()
        t.run_daily_job()
        logger.info('TushareFetch run_daily_job end')
        logger.info('ProcessStockData run_daily_job start')
        p = ProcessStockData()
        p.run_daily_job()
        logger.info('ProcessStockData run_daily_job end')
        logger.info('EmotionIndex run_daily_job start')
        e = EmotionIndex()
        e.run_daily_job()
        logger.info('EmotionIndex run_daily_job end')

    def repeat_daily_job(self, trade_date):
        """
        重复执行某天日常任务
        """
        print('repeat_daily_job')
        t = TushareFetch()
        t.repeat_daily_job(trade_date)
        p = ProcessStockData()
        p.repeat_daily_job(trade_date)
        e = EmotionIndex()
        e.repeat_daily_job(trade_date)

    def repeat_emotion_job(self, trade_date):
        """
        先下载沪深港通数据，在重复计算某日的情绪指标
        """
        self.fix_hsgt_data(trade_date)
        e = EmotionIndex()
        e.repeat_daily_job(trade_date)

    def fix_hsgt_data(self, trade_date):
        """
        修复沪深港通数据
        """
        print('fix_hsgt_data')
        t = TushareFetch()
        t.only_once_hsgt_data(trade_date)

    def fix_emotion_data(self, trade_date):
        """
        修复某日情绪指标
        """
        e = EmotionIndex()
        e.repeat_daily_job(trade_date)

    def check_hsgt_data(self, trade_date):
        """
        检测沪深港通某日数据是否存在
        """
        t = TushareFetch()
        t.check_hsgt_data(trade_date)

    def show_300_plot(self):
        """
        画出沪深300走势图
        """
        filename = os.path.join(const.origin_data_index_day_path,
                                const.CODE_INDEX_300 + '.csv')
        stock_data = pd.read_csv(filename)
        stock_data['trade_date'] = pd.to_datetime(
            stock_data['trade_date'], format='%Y%m%d')
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

        filename = os.path.join(const.emotion_index_data_root_path,
                                'emotion_index.csv')
        stock_data = pd.read_csv(filename)
        stock_data['trade_date'] = pd.to_datetime(
            stock_data['trade_date'], format='%Y%m%d')
        #     print(stock_data)
        stock_data.set_index('trade_date')

        fig, ax1 = plt.subplots()
        ax1.plot(
            stock_data['trade_date'], stock_data['close'], 'r', label='close')
        plt.legend(loc=2)

        ax2 = ax1.twinx()
        ax2.plot(
            stock_data['trade_date'], stock_data['v_5'], label='emotion_index')
        plt.legend(loc=1)

        ax1.set_xlabel('trade_date')
        ax1.set_ylabel('close')
        ax2.set_ylabel('emotion_index')
        plt.gcf().autofmt_xdate()

        plt.show()

    def show_hsgt_plot(self):
        """
        画出沪深港通数据走势图，叠加收盘价走势
        """
        plt.rcParams['axes.unicode_minus'] = False  # 用来正常显示负号

        filename = os.path.join(const.emotion_index_data_root_path,
                                'emotion_basic.csv')
        stock_data = pd.read_csv(filename)
        stock_data['trade_date'] = pd.to_datetime(
            stock_data['trade_date'], format='%Y%m%d')
        #     print(stock_data)
        stock_data.set_index('trade_date')

        fig, ax1 = plt.subplots()
        ax1.plot(
            stock_data['trade_date'], stock_data['close'], 'r', label='close')
        plt.legend(loc=2)

        ax2 = ax1.twinx()
        ax2.plot(
            stock_data['trade_date'],
            stock_data['north_money'],
            label='north_money')
        plt.legend(loc=1)

        ax1.set_xlabel('trade_date')
        ax1.set_ylabel('close')
        ax2.set_ylabel('north_money')
        plt.gcf().autofmt_xdate()

        plt.show()

    def select_rise_limit_times_31_5(self):
        """
        筛选出最近31天内5板的股票
        """
        select = SelectStock()
        select.select_rise_limit_times(31, 5)

    def select_rise_limit_times_60_5(self):
        """
        筛选出最近60天内5板的股票
        """
        select = SelectStock()
        select.select_rise_limit_times(60, 5)


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print('请输入要执行的方法名！')
        sys.exit()
    hutu_trade = HutuTrade()
    func = sys.argv[1]
    if func == 'run_only_once':
        hutu_trade.run_only_once()
    elif func == 'run_daily_job':
        hutu_trade.run_daily_job()
    elif func == 'fix_hsgt_data':
        if len(sys.argv) < 3:
            print('请输入trade_date参数值！')
            sys.exit()
        trade_date = sys.argv[2]
        hutu_trade.fix_hsgt_data(trade_date)
    elif func == 'repeat_daily_job':
        if len(sys.argv) < 3:
            print('请输入trade_date参数值！')
            sys.exit()
        trade_date = sys.argv[2]
        hutu_trade.repeat_daily_job(trade_date)
    elif func == 'repeat_emotion_job':
        if len(sys.argv) < 3:
            print('请输入trade_date参数值！')
            sys.exit()
        trade_date = sys.argv[2]
        hutu_trade.repeat_emotion_job(trade_date)
    elif func == 'show_emotion_plot':
        hutu_trade.show_emotion_plot()
    elif func == 'fix_emotion_data':
        if len(sys.argv) < 3:
            print('请输入trade_date参数值！')
            sys.exit()
        trade_date = sys.argv[2]
        hutu_trade.fix_emotion_data(trade_date)
    elif func == 'show_hsgt_plot':
        hutu_trade.show_hsgt_plot()
    elif func == 'check_hsgt_data':
        if len(sys.argv) < 3:
            print('请输入trade_date参数值！')
            sys.exit()
        trade_date = sys.argv[2]
        hutu_trade.check_hsgt_data(trade_date)
    elif func == 'select_rise_limit_times_31_5':
        hutu_trade.select_rise_limit_times_31_5()
    elif func == 'select_rise_limit_times_60_5':
        hutu_trade.select_rise_limit_times_60_5()
