from hutu import Hutu
import pandas as pd
import numpy as np
import time
from datetime import datetime
import os
import stock_const as const
import utility


class EmotionIndex(Hutu):
    '情绪指标'
    # 日期 收盘价 涨跌幅(%) 成交量(亿)  EXPMA24日线价格  120天价格  250天价格
    trade_date, close, pct_chg, vol, ema24, ma120, ma250 = [None] * 7
    # 上涨数 下跌数 涨停数  跌停数  连板数
    rise, fall, rise_limit, fall_limit, rise_limit_count = [None] * 5
    # 股价在24日线上的股票总数  股价在120天线上的股票总数
    ema24_up, ma120_up = [None] * 2
    # 250天内股价创新高的股票总数  250天内股价创新低的股票总数  北向资金流入 两融余额净买入
    day250_high_count, day250_low_count, north_money, rzrq_moeny = [None] * 4

    def __init__(self):
        super(EmotionIndex, self).__init__()
        self.check_folder()
        # 这里初始化上次更新日期为 emotion 目录下的上证指数文件最后更新日期
        self.last_update_time = self.get_last_update_time(self.hutu_type['emotion'])

    def check_folder(self):
        """
        检测目录结构，不存在就创建
        """
        # emotion_index 目录
        if not os.path.exists(const.emotion_index_data_root_path):
            os.makedirs(const.emotion_index_data_root_path)
            print('mkdir %s' % const.emotion_index_data_root_path)
        if not os.path.exists(const.emotion_index_data_day_path):
            os.makedirs(const.emotion_index_data_day_path)
            print('mkdir %s' % const.emotion_index_data_day_path)
        print('emotion_index 目录检测完毕！')

    @utility.time_it
    def run_only_once(self):
        """
        计算全部日期市场情绪指标的基础值，输入文件：处理后的带普通指标的股票文件，输出：
        """
        # 读取指数文件，取相应的值
        print('\n=====EmotionIndex run_only_once start=====', end='\n')
        print('开始时间：%s' % datetime.now(), end='\n')
        if not self.debug:
            df = pd.read_csv(const.ORIGIN_DATA_STOCK_TRADE_CAL)
        else:
            df = pd.read_csv(const.DEBUG_DATA_STOCK_TRADE_CAL)
        # # 初始化市场情绪基本指标
        # self.only_once_emotion_index_basic()
        # df = df[(df['cal_date'] > 20050101) & (df['cal_date'] < 20141211) & (df['is_open'] > 0)]
        # 市场情绪指标从20050101开始
        df = df[(df['cal_date'] > 20050101) & (df['is_open'] > 0)]
        # 升序排列日期
        df = df.sort_values(by=['cal_date'])
        # print(df)
        # 读取样本文件，获取数据格式
        emotion_df = pd.read_csv(const.SAMPLE_EMOTION_BASIC)
        emotion_df.drop(emotion_df.index, inplace=True)
        filename = os.path.join(const.emotion_index_data_root_path, 'emotion_basic.csv')
        count = 1
        length = len(df)
        for index, row in df.iterrows():
            emotion_df = emotion_df.append(self.calculate(row['cal_date']))
            # print(emotion_df)
            emotion_df = emotion_df.sort_values(by=['trade_date'], ascending=False)
            emotion_df.to_csv(filename, index=False, columns=const.EMOTION_BASIC_COLUMNS)
            percent = round(1.00 * count / length * 100, 2)
            print('计算日期：%s, 进度 : %s [%d/%d]' % ((row['cal_date'], str(percent)+'%', count, length)), end='\r')
            count = count + 1
        time.sleep(1)
        # 计算市场情绪指标文件
        self.compute_emotion_index()
        print('\n结束时间：%s' % datetime.now(), end='\n')
        print('=====EmotionIndex run_only_once done!=====', end='\n')

    @utility.time_it
    def run_daily_job(self):
        """
        计算市场情绪指标，日常任务
        """
        # 读取指数文件，取相应的值
        print('\n=====EmotionIndex run_daily_job start=====', end='\n')
        print('开始时间：%s' % datetime.now(), end='\n')
        # 计算从 last_update_time 到今天以来的需要更新日期
        date_list = self.get_update_duration()
        count = 1
        length = len(date_list)
        for date in date_list:
            # 更新emotion_basic文件
            self.update_emotion_basic(date)
            percent = round(1.00 * count / length * 100, 2)
            print('计算日期：%s, 进度 : %s [%d/%d]' % ((date, str(percent)+'%', count, length)), end='\r')
            count = count + 1
        time.sleep(1)
        # 计算市场情绪指标文件
        self.compute_emotion_index()
        print('\n结束时间：%s' % datetime.now(), end='\n')
        print('=====EmotionIndex run_daily_job done!=====', end='\n')

    @utility.time_it
    def update_emotion_basic(self, trade_date):
        """
        计算某日市场情绪指标的基础值
        """
        # 读取指数文件，取相应的值
        # print('\n=====计算某日市场情绪指标的基础值=====', end='\n')
        # print('开始时间：%s' % datetime.now(), end='\n')
        # 读取emotion_basic文件
        filename = os.path.join(const.emotion_index_data_root_path, 'emotion_basic.csv')
        emotion_df = pd.read_csv(filename)
        # print('计算日期 %s' % trade_date, end='\n')
        # 将数据按照交易日期升序
        emotion_df = emotion_df.sort_values(by=['trade_date'])
        emotion_df = emotion_df.append(self.calculate(trade_date))
        # 将数据按照交易日期降序
        emotion_df = emotion_df.sort_values(by=['trade_date'], ascending=False)
        # print(emotion_df)

        emotion_df.to_csv(filename, index=False, columns=const.EMOTION_BASIC_COLUMNS)
        # print('\n结束时间：%s' % datetime.now(), end='\n')
        # print('=====计算某日市场情绪指标的基础值 done!=====', end='\n')

    def compute_emotion_index(self):
        """
        计算市场情绪指标
        输入：emotion_basic文件
        输出：emotion_index文件
        """
        filename = os.path.join(const.emotion_index_data_root_path, 'emotion_basic.csv')
        if not os.path.exists(filename):
            print('文件不存在！%s' % filename)
            return False
        emotion_df = pd.read_csv(filename)
        # 升序排列
        emotion_df = emotion_df.sort_values(by=['trade_date'])
        # 分别计算各个值，分配权重
        emotion_df['pct_chg_v'] = round(emotion_df['pct_chg'] / 1.5, 2) * 5
        emotion_df['vol_5'] = emotion_df['vol'].rolling(5).mean()
        emotion_df['vol_v'] = round(emotion_df['vol'] / emotion_df['vol_5'], 2) * 3
        emotion_df['ema24_on_v'] = np.where(emotion_df['close'] > emotion_df['ema24'], 1, -1) * 5
        emotion_df['ma120_on_v'] = np.where(emotion_df['close'] > emotion_df['ma120'], 1, -1) * 4
        emotion_df['ma250_on_v'] = np.where(emotion_df['close'] > emotion_df['ma250'], 1, -1) * 3
        emotion_df['rise_fall_v'] = round((emotion_df['rise'] - emotion_df['fall']) / (emotion_df['rise'] + emotion_df['fall']), 2) * 3
        emotion_df['rise_limit_fall_limit_v'] = round((emotion_df['rise_limit'] - emotion_df['fall_limit']) / (emotion_df['rise_limit'] + emotion_df['fall_limit']), 2) * 3
        emotion_df['rise_limit_count_5'] = emotion_df['rise_limit_count'].rolling(5).mean()
        emotion_df['rise_limit_count_v'] = round(emotion_df['rise_limit_count'] / emotion_df['rise_limit_count_5'], 2) * 3
        emotion_df['ema24_up_5'] = emotion_df['ema24_up'].rolling(5).mean()
        emotion_df['ema24_up_v'] = round(emotion_df['ema24_up'] / emotion_df['ema24_up_5'], 2) * 4
        emotion_df['ma120_up_5'] = emotion_df['ma120_up'].rolling(5).mean()
        emotion_df['ma120_up_v'] = round(emotion_df['ma120_up'] / emotion_df['ma120_up_5'], 2) * 3
        # 用 0 填补 Nan
        emotion_df = emotion_df.fillna(0)
        # 最后计算平均值
        emotion_df['v'] = emotion_df['pct_chg_v'] + emotion_df['vol_v'] + emotion_df['ema24_on_v'] + emotion_df['ma120_on_v'] + emotion_df['ma250_on_v']
        emotion_df['v'] = emotion_df['v'] + emotion_df['rise_fall_v'] + emotion_df['rise_limit_fall_limit_v'] + emotion_df['rise_limit_count_v'] + emotion_df['ema24_up_v'] + emotion_df['ma120_up_v'] 
        emotion_df['v'] = round(emotion_df['v']/10, 2)
        emotion_df['v_5'] = emotion_df['v'].rolling(5).mean()
        p_filename = os.path.join(const.emotion_index_data_root_path, 'emotion_index.csv')
        columns = const.EMOTION_INDEX_COLUMNS.extend(const.EMOTION_BASIC_COLUMNS)
        print(columns)
        # 降序排列
        emotion_df = emotion_df.sort_values(by=['trade_date'], ascending=False)
        # print(emotion_df)
        emotion_df.to_csv(p_filename, index=False, columns=columns)        

    def calculate(self, trade_date):
        """
        按日计算市场情绪指标，输入文件都是处理后的股票文件
        先打开所有股票文件，取每个文件的trade_date一行数据，然后拼接成一个df，进行计算
        最后输出一个trade_date 统计过的市场情绪基础指标
        """
        # 输入股票列表文件，对每个股票文件循环取相同交易日期的行，然后拼接成一个新的df在计算
        if not self.debug:
            stock_list = pd.read_csv(const.ORIGIN_DATA_STOCK_BASIC)
        else:
            stock_list = pd.read_csv(const.DEBUG_DATA_STOCK_BASIC)
        count = 1
        # 读取处理过的第一个文件，要文件结构而已
        filename = os.path.join(const.process_data_market_day_path,  '000001.SZ.csv')
        tmp_df = pd.read_csv(filename)
        # print(tmp_df)
        tmp_df.drop(tmp_df.index, inplace=True)
        # print(tmp_df)
        length = len(stock_list)
        for index, row in stock_list.iterrows():
            filename = os.path.join(const.process_data_market_day_path, row["ts_code"] + '.csv')
            if os.path.exists(filename):
                df = pd.read_csv(filename)
                df = df[(df['trade_date'] == int(trade_date))]
                if (len(df) > 0):
                    tmp_df = tmp_df.append(df)
                    # print(tmp_df)
                    # print('文件：%s' % filename, end='\r')
            # percent = round(1.00 * count / length * 100, 2)
            # print('计算日期：%s, 进度 : %s [%d/%d]，code:%s' % ((trade_date, str(percent)+'%', count, length, row["ts_code"])), end='\r')
            count = count + 1
        # print(tmp_df)
        self.rise = tmp_df['rise'].sum()
        self.fall = tmp_df['fall'].sum()
        self.rise_limit = tmp_df['rise_limit'].sum()
        self.fall_limit = tmp_df['fall_limit'].sum()
        self.rise_limit_count = len(tmp_df[(tmp_df['rise_limit_count'] > 1)])
        self.ema24_up = len(tmp_df[(tmp_df['ema24_up'] > 0)])
        self.ma120_up = len(tmp_df[(tmp_df['ma120_up'] > 0)])
        
        # 输入指数文件，取相应的值
        filename = os.path.join(const.process_data_index_day_path, const.CODE_INDEX_SH + '.csv')
        df = pd.read_csv(filename)
        df = df[(df['trade_date'] == int(trade_date))]
        if (len(df) > 0):
            self.trade_date = df['trade_date'].values[0]
            self.close = df['close'].values[0]
            self.pct_chg = df['pct_chg'].values[0]
            self.vol = df['vol'].values[0]
            self.ema24 = df['ema24'].values[0]
            self.ma120 = df['ma120'].values[0]
            self.ma250 = df['ma250'].values[0]
      
        output_df = pd.DataFrame(
            {
                'trade_date': [self.trade_date],
                'close': [self.close],
                'pct_chg': [self.pct_chg],
                'vol': [self.vol],
                'ema24': [self.ema24],
                'ma120': [self.ma120],
                'ma250': [self.ma250],
                'rise': [self.rise],
                'fall': [self.fall],
                'rise_limit': [self.rise_limit],
                'fall_limit': [self.fall_limit],
                'rise_limit_count': [self.rise_limit_count],
                'ema24_up': [self.ema24_up],
                'ma120_up': [self.ma120_up]
            })
        # print(output_df)
        return output_df

    # @utility.time_it
    # def only_once_emotion_index_basic(self):
    #     """
    #     初始化市场情绪基本指标，为下一步计算市场情绪指标打基础
    #     """
    #     print('\n=====初始化市场情绪基本指标=====', end='\n')
    #     print('开始时间：%s' % datetime.now())
    #     if not self.debug:
    #         stock_list = pd.read_csv(const.ORIGIN_DATA_STOCK_BASIC)
    #     else:
    #         stock_list = pd.read_csv(const.DEBUG_DATA_STOCK_BASIC)
    #     count = 1
    #     for index, row in stock_list.iterrows():
    #         code = row["ts_code"]
    #         o_filename = os.path.join(const.process_data_market_day_path, row["ts_code"] + '.csv')
    #         p_filename = os.path.join(const.emotion_index_data_day_path, row["ts_code"] + '.csv')
    #         length = len(stock_list)
    #         if os.path.exists(o_filename):
    #             stock_data = pd.read_csv(o_filename)
    #             stock_data = self.compute_emotion_index_basic(stock_data)
    #             print(stock_data)
    #             # const.COLUMNS.extend(const.INDICATOR_COLUMNS)
    #             stock_data.to_csv(p_filename, index=False, columns=const.EMOTION_BASIC_COLUMNS)
    #         percent = round(1.00 * count / length * 100, 2)
    #         print('进度 : %s [%d/%d]，code:%s' % ((str(percent)+'%', count, length, code)), end='\r')

    #         count = count + 1
    #     print('结束时间：%s' % datetime.now())
    #     print('=====初始化市场情绪基本指标 done!=====', end='\n')

    # def compute_emotion_index_basic(self, stock_data):
    #     """
    #     计算市场情绪基本指标，输入为单只股票的日行情
    #     """
    #     # 将数据按照交易日期从远到近排序
    #     df = stock_data.sort_values(by=['trade_date'])
    #     df = df.reset_index(drop=True)
    #     # 上涨
    #     df['rise'] = np.where(df['pct_chg'] > 0, 1, 0)
    #     # 下跌
    #     df['fall'] = np.where(df['pct_chg'] < 0, 1, 0)
    #     # 涨停
    #     df['rise_limit'] = np.where(round(df["pre_close"] * 1.1, 2) == df['close'], 1, 0)
    #     # 涨停连板
    #     df['rise_limit_count'] = df['rise_limit']
    #     # 跌停
    #     df['fall_limit'] = np.where(round(df["pre_close"] * 0.9, 2) == df['close'], 1, 0)
    #     # 跌停连板
    #     df['fall_limit_count'] = df['fall_limit']
    #     # ema24日线上方
    #     df['ema24_up'] = np.where(df["close"] >= df['ema24'], 1, 0)
    #     # ma120日线上方
    #     df['ma120_up'] = np.where(df["close"] >= df['ma120'], 1, 0)

    #     rise_limit_count = 0
    #     fall_limit_count = 0
    #     for index, row in df.iterrows():
    #         # print(type(row))
    #         # 计算涨停板连板
    #         # print(row["ts_code"], row['trade_date'], row['close'], row["pre_close"], row['pct_chg'])
    #         if (row['rise_limit'] == 1):
    #             rise_limit_count = rise_limit_count + 1
    #         else:
    #             rise_limit_count = 0
    #         df.at[index, 'rise_limit_count'] = rise_limit_count
    #         # 计算跌停板连板
    #         if (row['fall_limit'] == 1):
    #             fall_limit_count = fall_limit_count + 1
    #         else:
    #             fall_limit_count = 0
    #         df.at[index, 'fall_limit_count'] = fall_limit_count
    #         # # 计算30天内250日的股价高点
    #         # # print(df[0:index].tail())
    #         # is_high = self.compute_30_250_high(str(row['trade_date']), row['high'], df[0:index])
    #         # if is_high:
    #         #     df.at[index, 'is_high_30_250'] = 1
    #         # else:
    #         #     df.at[index, 'is_high_30_250'] = 0
    #     # 将数据按照交易日期从近到远排序
    #     df = df.sort_values(by=['trade_date'], ascending=False)
    #     return df

    # @utility.time_it
    # def daily_job_continued_limit_count(self):
    #     """
    #     更新连板数等数据，每日执行
    #     """
    #     print('\n=====更新连板数等数据=====', end='\n')
    #     print('开始时间：%s' % datetime.now())
    #     if not self.debug:
    #         stock_list = pd.read_csv(const.ORIGIN_DATA_STOCK_BASIC)
    #     else:
    #         stock_list = pd.read_csv(const.DEBUG_DATA_STOCK_BASIC)
    #     count = 1
    #     for index, row in stock_list.iterrows():
    #         o_filename = os.path.join(const.process_data_market_day_path, row["ts_code"] + '.csv')
    #         # p_filename = os.path.join(const.process_data_market_day_path, row["ts_code"] + '.csv')
    #         self.show_continued_limit_count(
    #             o_filename,
    #             o_filename,
    #             row["ts_code"],
    #             count,
    #             len(stock_list)
    #             )
    #         count = count + 1
    #     print('结束时间：%s' % datetime.now())
    #     print('=====更新连板数等数据 done!=====', end='\n')

    # def compute_30_250_high(self, date, date_high, df):
    #     """
    #     30天内250日的股价高点值
    #     1、先转换到30天周期，在转换到250天周期，得到df30与df250
    #     2、然后用当前日期减去30天得到日期X
    #     3、X 落在df250的日期范围内
    #     4、比较df30.high 与 date.high 的高值，在 与 x 所在行的 df250.high
    #     5、如果高则表示创新高，否则没有创新高
    #     """
    #     result = False
    #     # 这里要注意，这里的周期是自然日历的周期，250个交易日，应该是365天才对
    #     df30 = self.trans2period(df, '30D')
    #     df250 = self.trans2period(df, '365D')
    #     # print('%s %s' % (date, date_high))
    #     if (len(df30) > 0 and len(df250) > 0):
    #         # print('=====df30=====')
    #         # print(df30.tail())
    #         # print('=====df250=====')
    #         # print(df250.tail())
    #         # date = '20180108'
    #         x_date = datetime.strptime(date, '%Y%m%d')
    #         x_date_period = x_date + timedelta(days=-30)
    #         # print(x_date_period)
    #         # x_date_period_int = int(datetime.strftime(x_date_period, '%Y%m%d'))
    #         # df30_new = df30[(date>=df30['date']) & (int(date)<=df30['trade_date'])]
    #         df30_new = df30.tail(1)
    #         # print('=====df30_new=====')
    #         # print(df30_new.tail())
    #         df250_new = df250[(x_date_period>=df250['date']) & (int(datetime.strftime(x_date_period, '%Y%m%d'))<=df250['trade_date'])]
    #         # df250_new = df250.tail(1)
    #         # print('=====df250_new=====')
    #         # print(df250_new.tail())
    #         if (len(df30_new) > 0 and len(df250_new) > 0):                
    #             result = max(date_high, df30_new['high'].values[0]) >= df250_new['high'].values[0]
    #             print('%s %s %s %s %s' % (date, date_high, df30_new['high'].values[0], df250_new['high'].values[0], result))
    #             # result = date_high >= df250_new['high'].values[0]
    #     return result

    # def trans2period(self, stock_data, period_type):
    #     # period_type = 'W'  # 日D  周W  月M  季Q  年A  小时H  分钟T
    #     stock_data = stock_data.sort_values(by=['trade_date'], ascending=False)
    #     # print(stock_data)
    #     stock_data['date'] = pd.to_datetime(stock_data['trade_date'], format='%Y%m%d')
    #     stock_data.set_index('date', inplace=True)        
    #     period_stock_data = stock_data.resample(period_type).last()
    #     period_stock_data['open'] = stock_data['open'].resample(period_type).first()
    #     period_stock_data['high'] = stock_data['high'].resample(period_type).max()
    #     period_stock_data['low'] = stock_data['low'].resample(period_type).min() 
    #     period_stock_data['change'] = stock_data['change'].resample(period_type).sum()
    #     period_stock_data['pre_close'] = stock_data['pre_close'].resample(period_type).first()
    #     period_stock_data['pct_chg'] = round((period_stock_data['close'] - period_stock_data['pre_close']) / period_stock_data['pre_close'], 4) * 100
    #     period_stock_data['vol'] = stock_data['vol'].resample(period_type).sum()
    #     period_stock_data['amount'] = stock_data['amount'].resample(period_type).sum()
    #     period_stock_data = period_stock_data[period_stock_data['ts_code'].notnull()]
    #     period_stock_data.reset_index(inplace=True)
    #     return period_stock_data        


# e = EmotionIndex()
# e.run_only_once()