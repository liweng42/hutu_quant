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
        self.last_update_time = self.get_last_update_time(
            self.hutu_type['emotion'])

    def check_folder(self):
        """
        检测目录结构，不存在就创建
        """
        # emotion_index_data 目录
        if not os.path.exists(const.emotion_index_data_root_path):
            os.makedirs(const.emotion_index_data_root_path)
            print('mkdir %s' % const.emotion_index_data_root_path)
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
        date_list = []
        for index, row in df.iterrows():
            date_list.append(row['cal_date'])
        date_list.sort()
        # 判断当前时间，如果在下午16点20分后，可以计算当天日期，否则排除当天日期
        d1 = datetime.strptime(
            str(datetime.now().date()) + '16:20', '%Y-%m-%d%H:%M')
        if datetime.now() < d1 and int(self.today_date) in date_list:
            date_list.remove(int(self.today_date))
        # print(df)
        # 读取样本文件，获取数据格式
        emotion_df = pd.read_csv(const.SAMPLE_EMOTION_BASIC)
        emotion_df.drop(emotion_df.index, inplace=True)
        filename = os.path.join(const.emotion_index_data_root_path,
                                'emotion_basic.csv')
        count = 1
        length = len(date_list)
        for date in date_list:
            emotion_df = emotion_df.append(self.calculate(date))
            # print(emotion_df)
            emotion_df = emotion_df.sort_values(
                by=['trade_date'], ascending=False)
            emotion_df.to_csv(
                filename, index=False, columns=const.EMOTION_BASIC_COLUMNS)
            percent = round(1.00 * count / length * 100, 2)
            print(
                '计算日期：%s, 进度 : %s [%d/%d]' % (
                    (date, str(percent) + '%', count, length)),
                end='\r')
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
            time.sleep(0.5)
            percent = round(1.00 * count / length * 100, 2)
            print(
                '计算日期：%s, 进度 : %s [%d/%d]' % (
                    (date, str(percent) + '%', count, length)),
                end='\r')
            count = count + 1
        time.sleep(1)
        # 计算市场情绪指标文件
        self.compute_emotion_index()
        print('\n结束时间：%s' % datetime.now(), end='\n')
        print('=====EmotionIndex run_daily_job done!=====', end='\n')

    @utility.time_it
    def repeat_daily_job(self, trade_date):
        """
        计算市场情绪指标，日常任务
        """
        # 读取指数文件，取相应的值
        print('\n=====EmotionIndex repeat_daily_job start=====', end='\n')
        print('开始时间：%s' % datetime.now(), end='\n')
        trade_date = int(trade_date)
        # 更新emotion_basic文件
        self.update_emotion_basic(trade_date)
        # 计算市场情绪指标文件
        self.compute_emotion_index()
        print('\n结束时间：%s' % datetime.now(), end='\n')
        print('=====EmotionIndex repeat_daily_job done!=====', end='\n')

    @utility.time_it
    def update_emotion_basic(self, trade_date):
        """
        计算某日市场情绪指标的基础值
        """
        # 读取指数文件，取相应的值
        # print('\n=====计算某日市场情绪指标的基础值=====', end='\n')
        # print('开始时间：%s' % datetime.now(), end='\n')
        # 读取emotion_basic文件
        filename = os.path.join(const.emotion_index_data_root_path,
                                'emotion_basic.csv')
        emotion_df = pd.read_csv(filename)
        # print('计算日期 %s' % trade_date, end='\n')
        # 将数据按照交易日期升序
        emotion_df = emotion_df.sort_values(by=['trade_date'])
        # 检查是否存在该日期
        tmp_df = emotion_df[(emotion_df['trade_date'] == int(trade_date))]
        if (len(tmp_df) <= 0):  # 不存在则追加
            emotion_df = emotion_df.append(self.calculate(trade_date))
            # 将数据按照交易日期降序
            emotion_df = emotion_df.sort_values(
                by=['trade_date'], ascending=False)
            # print(emotion_df)
            emotion_df.to_csv(
                filename, index=False, columns=const.EMOTION_BASIC_COLUMNS)
        # print('\n结束时间：%s' % datetime.now(), end='\n')
        # print('=====计算某日市场情绪指标的基础值 done!=====', end='\n')

    def compute_emotion_index(self):
        """
        计算市场情绪指标
        输入：emotion_basic文件
        输出：emotion_index文件
        """
        filename = os.path.join(const.emotion_index_data_root_path,
                                'emotion_basic.csv')
        if not os.path.exists(filename):
            print('文件不存在！%s' % filename)
            return False
        emotion_df = pd.read_csv(filename)
        # 升序排列
        emotion_df = emotion_df.sort_values(by=['trade_date'])
        # 分别计算各个值，分配权重
        emotion_df['pct_chg_v'] = round(emotion_df['pct_chg'] / 1.5, 2) * 5
        emotion_df['vol_5'] = emotion_df['vol'].rolling(5).mean()
        emotion_df['vol_v'] = round(emotion_df['vol'] / emotion_df['vol_5'],
                                    2) * 3
        emotion_df['ema24_on_v'] = np.where(
            emotion_df['close'] > emotion_df['ema24'], 1, -1) * 5
        emotion_df['ma120_on_v'] = np.where(
            emotion_df['close'] > emotion_df['ma120'], 1, -1) * 4
        emotion_df['ma250_on_v'] = np.where(
            emotion_df['close'] > emotion_df['ma250'], 1, -1) * 3
        emotion_df['rise_fall_v'] = round(
            (emotion_df['rise'] - emotion_df['fall']) /
            (emotion_df['rise'] + emotion_df['fall']), 2) * 3
        emotion_df['rise_limit_fall_limit_v'] = round(
            (emotion_df['rise_limit'] - emotion_df['fall_limit']) /
            (emotion_df['rise_limit'] + emotion_df['fall_limit']), 2) * 3
        emotion_df['rise_limit_count_5'] = emotion_df[
            'rise_limit_count'].rolling(5).mean()
        emotion_df['rise_limit_count_v'] = round(
            emotion_df['rise_limit_count'] / emotion_df['rise_limit_count_5'],
            2) * 3
        emotion_df['ema24_up_5'] = emotion_df['ema24_up'].rolling(5).mean()
        emotion_df['ema24_up_v'] = round(
            emotion_df['ema24_up'] / emotion_df['ema24_up_5'], 2) * 4
        emotion_df['ma120_up_5'] = emotion_df['ma120_up'].rolling(5).mean()
        emotion_df['ma120_up_v'] = round(
            emotion_df['ma120_up'] / emotion_df['ma120_up_5'], 2) * 3
        emotion_df['north_money_5'] = emotion_df['north_money'].rolling(
            5).mean()
        # 先取今日北上资金是5日均值的几倍，然后根据今日正负决定方向，最高给 4，最低给 -4
        emotion_df['north_money_v1'] = round(
            abs(emotion_df['north_money']) / abs(emotion_df['north_money_5']),
            2)
        emotion_df['north_money_v'] = np.where(emotion_df['north_money'] > 0,
                                               emotion_df['north_money_v1'] * 3,
                                               -emotion_df['north_money_v1'] * 3)
        emotion_df['north_money_v'] = np.where(
            emotion_df['north_money_v'] >= 4, 4, emotion_df['north_money_v'])
        emotion_df['north_money_v'] = np.where(
            emotion_df['north_money_v'] <= -4, -4, emotion_df['north_money_v'])
        # 用 0 填补 Nan
        emotion_df = emotion_df.fillna(0)
        # 最后计算平均值，一共11个因子
        count = 11
        emotion_df['v'] = emotion_df['pct_chg_v'] + emotion_df[
            'vol_v'] + emotion_df['ema24_on_v'] + emotion_df[
                'ma120_on_v'] + emotion_df['ma250_on_v']
        emotion_df[
            'v'] = emotion_df['v'] + emotion_df['rise_fall_v'] + emotion_df[
                'rise_limit_fall_limit_v'] + emotion_df[
                    'rise_limit_count_v'] + emotion_df[
                        'ema24_up_v'] + emotion_df['ma120_up_v']
        emotion_df['v'] = emotion_df['v'] + emotion_df['north_money_v']
        emotion_df['v'] = round(emotion_df['v'] / count, 1)
        emotion_df['v_5'] = emotion_df['v'].rolling(5).mean()
        emotion_df['v_5'] = round(emotion_df['v_5'], 1)
        p_filename = os.path.join(const.emotion_index_data_root_path,
                                  'emotion_index.csv')
        columns = const.EMOTION_INDEX_COLUMNS.extend(
            const.EMOTION_BASIC_COLUMNS)
        print(columns)
        # 降序排列
        emotion_df = emotion_df.sort_values(by=['trade_date'], ascending=False)
        # print(emotion_df)
        emotion_df.to_csv(p_filename, index=False, columns=columns)

    def calculate(self, trade_date):
        """
        输入文件：trade_date维度的股票文件
        输出：指数日期的市场情绪基础指标值
        """
        filename = os.path.join(const.process_data_market_trade_date_day_path,
                                str(trade_date) + '.csv')
        df = pd.read_csv(filename)
        # print(tmp_df)
        # 统计市场情绪基础指标
        self.rise = df['rise'].sum()
        self.fall = df['fall'].sum()
        self.rise_limit = df['rise_limit'].sum()
        self.fall_limit = df['fall_limit'].sum()
        self.rise_limit_count = len(df[(df['rise_limit_count'] > 1)])
        self.ema24_up = len(df[(df['ema24_up'] > 0)])
        self.ma120_up = len(df[(df['ma120_up'] > 0)])

        # 输入指数文件，取相应的值
        filename = os.path.join(const.process_data_index_day_path,
                                const.CODE_INDEX_SH + '.csv')
        df = pd.read_csv(filename)
        df = df[(df['trade_date'] == int(trade_date))]
        if (len(df) > 0):
            self.trade_date = df['trade_date'].values[0]
            self.close = round(df['close'].values[0], 2)
            self.pct_chg = round(df['pct_chg'].values[0], 2)
            self.vol = df['vol'].values[0]
            self.ema24 = df['ema24'].values[0]
            self.ma120 = df['ma120'].values[0]
            self.ma250 = df['ma250'].values[0]

        # 输入沪深港通文件，取北向资金值
        df = pd.read_csv(const.ORIGIN_DATA_MONEYFLOW_HSGT)
        df = df[(df['trade_date'] == int(trade_date))]
        if (len(df) > 0):
            self.north_money = df['north_money'].values[0]
        else:
            self.north_money = 0

        output_df = pd.DataFrame({
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
            'ma120_up': [self.ma120_up],
            'north_money': [self.north_money]
        })
        # print(output_df)
        return output_df

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
