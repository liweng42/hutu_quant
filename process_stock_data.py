from hutu import Hutu
import pandas as pd
import time
from datetime import datetime, timedelta
import numpy as np
import talib
import os
import stock_const as const
import utility


class ProcessStockData(Hutu):
    '处理股票数据，输出处理后的文件，不对原文件修改'

    def __init__(self):
        super(ProcessStockData, self).__init__()
        self.check_folder()
        # 这里初始化上次更新日期为 process 目录下的上证指数文件最后更新日期
        self.last_update_time = self.get_last_update_time(self.hutu_type['process'])

    def check_folder(self):
        """
        检测目录结构，不存在就创建
        """
        # process_data 目录
        if not os.path.exists(const.process_data_basic_path):
            os.makedirs(const.process_data_basic_path)
            print('mkdir %s' % const.process_data_basic_path)
        if not os.path.exists(const.process_data_fin_path):
            os.makedirs(const.process_data_fin_path)
            print('mkdir %s' % const.process_data_fin_path)
        if not os.path.exists(const.process_data_index_day_path):
            os.makedirs(const.process_data_index_day_path)
            print('mkdir %s' % const.process_data_index_day_path)
        if not os.path.exists(const.process_data_index_week_path):
            os.makedirs(const.process_data_index_week_path)
            print('mkdir %s' % const.process_data_index_week_path)
        if not os.path.exists(const.process_data_market_day_path):
            os.makedirs(const.process_data_market_day_path)
            print('mkdir %s' % const.process_data_market_day_path)
        if not os.path.exists(const.process_data_market_week_path):
            os.makedirs(const.process_data_market_week_path)
            print('mkdir %s' % const.process_data_market_week_path)
        if not os.path.exists(const.process_data_ref_path):
            os.makedirs(const.process_data_ref_path)
            print('mkdir %s' % const.process_data_ref_path)
        if not os.path.exists(const.process_data_market_trade_date_day_path):
            os.makedirs(const.process_data_market_trade_date_day_path)
            print('mkdir %s' % const.process_data_market_trade_date_day_path)
        print('process_data 目录检测完毕！')

    @utility.time_it
    def run_only_once(self):
        """
        初始化处理所有数据
        """
        print('\n=====ProcessStockData run_only_once start=====', end='\n')
        print('开始时间：%s' % datetime.now(), end='\n')
        # 计算一般指标数据
        self.compute_stock_indicators()
        # 一定最后处理指数数据
        self.compute_index_indicators()
        # 生成trade_date维度的股票数据文件
        self.only_once_generate_trade_date_day_file()
        print('\n结束时间：%s' % datetime.now(), end='\n')
        print('=====ProcessStockData run_only_once done!=====', end='\n')

    @utility.time_it
    def run_daily_job(self):
        """
        日常任务，处理所有数据
        """
        # 计算从 last_update_time 到今天以来的需要更新日期
        date_list = self.get_update_duration()
        if len(date_list) > 0:
            # 计算一般指标数据
            self.compute_stock_indicators()
            # 一定最后处理指数数据
            self.compute_index_indicators()
            # 生成trade_date维度的股票数据文件
            count = 1
            length = len(date_list)
            print('\n=====generate_trade_date_day_file start=====', end='\n')
            for date in date_list:
                self.generate_trade_date_day_file(date)
                percent = round(1.00 * count / length * 100, 2)
                print('计算日期：%s, 进度 : %s [%d/%d]' % (date, str(percent)+'%', count, length), end='\r')
                count = count + 1
            print('\n=====generate_trade_date_day_file end=====', end='\n')
            return True
        else:
            print('\n没有需要处理的数据', end='\n')
            return False

    def compute_indicators(self, stock_data, is_index):
        """
        计算常见指标，包括ma/expma/macd/rsi， 数据格式要求必须有 trade_date close
        """
        # stock_data['date'] = pd.to_datetime(stock_data['trade_date'], format='%Y%m%d')
        # stock_data = stock_data.set_index('date', inplace=True)
        # 将数据按照交易日期从远到近排序
        stock_data = stock_data.sort_values(by=['trade_date'])
        # print(stock_data)
        close = [float(x) for x in stock_data['close']]                                      
        # 分别计算5日、20日、60日 120日 250日的移动平均线
        ma_list = [5, 20, 60, 120, 250]
        # 计算简单算术移动平均线MA - 注意：stock_data['close']为股票每天的收盘价
        for ma in ma_list:
            # stock_data['ma' + str(ma)] = stock_data['close'].rolling(ma).mean()
            # 调用talib计算日移动平均线的值
            # 这里要注意，有的新股上市日期短，无法计算出 5日以及其他均线价格，如果计算则 talib.ma会报错
            # Exception: inputs are all NaN            
            if (len(close) >= ma):
                stock_data['ma' + str(ma)] = talib.MA(np.array(close), timeperiod=ma)
                stock_data['ma' + str(ma)] = round(stock_data['ma' + str(ma)], 2)
            else:
                stock_data['ma' + str(ma)] = 0
        # 计算指数平滑移动平均线EMA
        ema_list = [17, 24, 50]
        for ema in ema_list:
            # stock_data['ema' + str(ema)] = stock_data['close'].ewm(span=ema).mean()    
            # 调用talib计算指数移动平均线的值
            if (len(close) >= ema):
                stock_data['ema' + str(ema)] = talib.EMA(np.array(close), timeperiod=ema)
                stock_data['ema' + str(ema)] = round(stock_data['ema' + str(ema)], 2)
            else:
                stock_data['ema' + str(ema)] = 0
        # 调用talib计算MACD指标
        # if (len(close) >= 26):
        #     stock_data['diff'], stock_data['dea'], stock_data['macd'] = talib.MACD(
        #         np.array(close),
        #         fastperiod=12,
        #         slowperiod=26,
        #         signalperiod=9
        #         )
        #     stock_data['diff'] = round(stock_data['diff'], 2)
        #     stock_data['dea'] = round(stock_data['dea'], 2)
        #     stock_data['macd'] = round(2*(round(stock_data['diff'], 3)-round(stock_data['dea'], 3)), 2)
        # else:
        #     stock_data['diff'] = 0
        #     stock_data['dea'] = 0
        #     stock_data['macd'] = 0
        stock_data['diff'], stock_data['dea'], stock_data['macd'] = talib.MACD(
            np.array(close),
            fastperiod=12,
            slowperiod=26,
            signalperiod=9
            )
        stock_data['diff'] = round(stock_data['diff'], 2)
        stock_data['dea'] = round(stock_data['dea'], 2)
        stock_data['macd'] = round(2*(round(stock_data['diff'], 3)-round(stock_data['dea'], 3)), 2)        
        # 调用talib计算rsi指标
        # if (len(close) >= 6):
        #     stock_data['rsi1'] = talib.RSI(np.array(close), timeperiod=6)
        #     stock_data['rsi1'] = round(stock_data['rsi1'], 2)
        # else:
        #     stock_data['rsi1'] = 0
        stock_data['rsi1'] = talib.RSI(np.array(close), timeperiod=6)
        stock_data['rsi1'] = round(stock_data['rsi1'], 2)
        # 不是指数计算下面的指标
        if not is_index:
            # 上涨
            stock_data['rise'] = np.where(stock_data['pct_chg'] > 0, 1, 0)
            # 下跌
            stock_data['fall'] = np.where(stock_data['pct_chg'] < 0, 1, 0)
            # 涨停
            stock_data['rise_limit'] = np.where(round(stock_data["pre_close"] * 1.1, 2) == stock_data['close'], 1, 0)
            # # 涨停连板
            stock_data['rise_limit_count'] = stock_data['rise_limit']
            # 跌停
            stock_data['fall_limit'] = np.where(round(stock_data["pre_close"] * 0.9, 2) == stock_data['close'], 1, 0)
            # # 跌停连板
            stock_data['fall_limit_count'] = stock_data['fall_limit']
            # ema24日线上方
            stock_data['ema24_up'] = np.where(stock_data["close"] >= stock_data['ema24'], 1, 0)
            # ma120日线上方
            stock_data['ma120_up'] = np.where(stock_data["close"] >= stock_data['ma120'], 1, 0)

            rise_limit_count = 0
            fall_limit_count = 0
            for index, row in stock_data.iterrows():
                # 计算涨停板连板
                if (row['rise_limit'] == 1):
                    rise_limit_count = rise_limit_count + 1
                else:
                    rise_limit_count = 0
                stock_data.at[index, 'rise_limit_count'] = rise_limit_count
                # 计算跌停板连板
                if (row['fall_limit'] == 1):
                    fall_limit_count = fall_limit_count + 1
                else:
                    fall_limit_count = 0
                stock_data.at[index, 'fall_limit_count'] = fall_limit_count

        # 将数据按照交易日期从近到远排序
        stock_data = stock_data.sort_values(by=['trade_date'], ascending=False)
        # stock_data = stock_data.fillna(0)
        return stock_data

    @utility.time_it
    def only_once_generate_trade_date_day_file(self):
        """
        生成以trade_date维度的文件，即每日一个股票文件，包含当日交易的所有股票数据
        """
        print('\n=====only_once_generate_trade_date_day_file start=====', end='\n')
        print('开始时间：%s' % datetime.now(), end='\n')
        date_list = self.get_cal_open_list()
        count = 1
        length = len(date_list)
        for date in date_list:
            self.generate_trade_date_day_file(date)
            percent = round(1.00 * count / length * 100, 2)
            print('计算日期：%s, 进度 : %s [%d/%d]' % (date, str(percent)+'%', count, length), end='\r')
            count = count + 1
        print('\n=====only_once_generate_trade_date_day_file end=====', end='\n')
        print('结束时间：%s' % datetime.now(), end='\n')

    def generate_trade_date_day_file(self, trade_date):
        """
        生成以trade_date维度的文件，即每日一个股票文件，包含当日交易的所有股票数据
        """
        # 输入股票列表文件，对每个股票文件循环取相同交易日期的行，然后拼接成一个新的df在计算
        if not self.debug:
            stock_list = pd.read_csv(const.ORIGIN_DATA_STOCK_BASIC)
        else:
            stock_list = pd.read_csv(const.DEBUG_DATA_STOCK_BASIC)

        # 读取处理过的第一个文件，要文件结构而已，此时的文件包含计算过的数据指标列
        filename = os.path.join(const.process_data_market_day_path,  '000001.SZ.csv')
        tmp_df = pd.read_csv(filename)
        # print(tmp_df)
        tmp_df.drop(tmp_df.index, inplace=True)
        # print(tmp_df)
        for index, row in stock_list.iterrows():
            filename = os.path.join(const.process_data_market_day_path, row["ts_code"] + '.csv')
            if os.path.exists(filename):
                df = pd.read_csv(filename)
                df = df[(df['trade_date'] == int(trade_date))]
                if (len(df) > 0):
                    tmp_df = tmp_df.append(df)
                    # print(tmp_df)
                    # print('文件：%s' % filename, end='\r')
        p_filename = os.path.join(const.process_data_market_trade_date_day_path, str(trade_date) + '.csv')
        if (len(tmp_df) > 0):
            tmp_df.to_csv(p_filename, index=False)
            # print(tmp_df)

    @utility.time_it
    def compute_index_indicators(self):
        """
        计算各指数常见指标，读取原始数据文件，计算后输出到处理后的文件
        """
        print('\n=====计算指数相关指标=====', end='\n')
        print('开始时间：%s' % datetime.now())
        count = 1
        for li in const.CODE_INDEX_LIST:
            o_filename = os.path.join(const.origin_data_index_day_path, li + '.csv')
            p_filename = os.path.join(const.process_data_index_day_path, li + '.csv')
            self.show_compute_index_indicators(
                o_filename,
                p_filename,
                li,
                count,
                len(const.CODE_INDEX_LIST),
                True
                )
            count = count + 1
        print('结束时间：%s' % datetime.now())
        print('=====计算指数日线相关指标 done!=====', end='\n')

    @utility.time_it
    def compute_stock_indicators(self):
        """
        计算股票常见指标
        """
        print('\n=====计算股票常见指标=====', end='\n')
        print('开始时间：%s' % datetime.now())
        if not self.debug:
            stock_list = pd.read_csv(const.ORIGIN_DATA_STOCK_BASIC)
        else:
            stock_list = pd.read_csv(const.DEBUG_DATA_STOCK_BASIC)
        count = 1
        for index, row in stock_list.iterrows():
            o_filename = os.path.join(const.origin_data_market_day_path, row["ts_code"] + '.csv')
            p_filename = os.path.join(const.process_data_market_day_path, row["ts_code"] + '.csv')
            self.show_compute_index_indicators(
                o_filename,
                p_filename,
                row["ts_code"],
                count,
                len(stock_list),
                False
                )
            count = count + 1
        print('结束时间：%s' % datetime.now())
        print('=====计算股票常见指标 done!=====', end='\n')

    def show_compute_index_indicators(self, o_filename, p_filename, code, count, length, is_index):
        """
        显示处理进度
        """
        if os.path.exists(o_filename):
            stock_data = pd.read_csv(o_filename)
            stock_data = self.compute_indicators(stock_data, is_index)
            columns = const.COLUMNS.extend(const.INDICATOR_COLUMNS)
            stock_data.to_csv(p_filename, index=False, columns=columns)
        percent = round(1.00 * count / length * 100, 2)
        print('进度 : %s [%d/%d]，code:%s' % ((str(percent)+'%', count, length, code)), end='\r')

    def trans_day2week(self, code):
        """
        将日线转换成周线
        """
        period_type = 'W'
        name = 'tick_data_%s.csv' % code
        filename = './day/%s' % name
        stock_data = pd.read_csv(filename)
        stock_data['date'] = pd.to_datetime(stock_data['trade_date'], format='%Y%m%d')
        stock_data.set_index('date', inplace=True)
        # print(stock_data)
        # stock_data.info()
        period_stock_data = stock_data.resample(period_type, how='last')
        period_stock_data['open'] = stock_data['open'].resample(period_type, how='first')
        period_stock_data['high'] = stock_data['high'].resample(period_type, how='max')
        period_stock_data['low'] = stock_data['low'].resample(period_type, how='min')    
        period_stock_data['change'] = stock_data['change'].resample(period_type, how='sum')
        period_stock_data['pre_close'] = stock_data['pre_close'].resample(period_type, how='first')
        period_stock_data['pct_chg'] = round((period_stock_data['close'] - period_stock_data['pre_close']) / period_stock_data['pre_close'], 4) * 100
        period_stock_data['vol'] = stock_data['vol'].resample(period_type, how='sum')
        period_stock_data['amount'] = stock_data['amount'].resample(period_type, how='sum')
        period_stock_data = period_stock_data[period_stock_data['ts_code'].notnull()]
        period_stock_data.reset_index(inplace=True)
        # print(period_stock_data)
        filename = './week/%s' % name
        period_stock_data.to_csv(filename, index=False, columns=['ts_code', 'trade_date', 'open', 'high', 'low', 'close', 'pre_close', 'change', 'pct_chg', 'vol', 'amount'])

    def trans_all_week(self):
        """
        获取全部股票的全部日线
        """
        stock_list = pd.read_csv(const.ORIGIN_DATA_STOCK_BASIC)
        for index, row in stock_list.iterrows():
            print(row["ts_code"], row['symbol'], row['name'], row["list_date"])
            self.trans_day2week(row['symbol'])