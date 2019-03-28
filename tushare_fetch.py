from hutu import Hutu
import tushare as ts
import stock_const as const
import time
from datetime import datetime
import os
import pandas as pd
import utility


class TushareFetch(Hutu):
    '使用tushare接口获得股票数据，写入原始文件'
    # tushare 要求的api token
    api_token = '0b295352f360d571aabad80f7d8980094306e6d03e6e3f4f55a8b29b'
    ts.set_token(api_token)
    pro = ts.pro_api()

    def __init__(self):
        super(TushareFetch, self).__init__()
        self.check_folder()
        # 这里初始化上次更新日期为 process 目录下的上证指数文件最后更新日期
        self.last_update_time = self.get_last_update_time(
            self.hutu_type['process'])

    def check_folder(self):
        """
        检测目录结构，不存在就创建
        """
        # origin_data 目录
        if not os.path.exists(const.origin_data_basic_path):
            os.makedirs(const.origin_data_basic_path)
            print('mkdir %s' % const.origin_data_basic_path)
        if not os.path.exists(const.origin_data_fin_path):
            os.makedirs(const.origin_data_fin_path)
            print('mkdir %s' % const.origin_data_fin_path)
        if not os.path.exists(const.origin_data_index_day_path):
            os.makedirs(const.origin_data_index_day_path)
            print('mkdir %s' % const.origin_data_index_day_path)
        if not os.path.exists(const.origin_data_index_week_path):
            os.makedirs(const.origin_data_index_week_path)
            print('mkdir %s' % const.origin_data_index_week_path)
        if not os.path.exists(const.origin_data_market_day_path):
            os.makedirs(const.origin_data_market_day_path)
            print('mkdir %s' % const.origin_data_market_day_path)
        if not os.path.exists(const.origin_data_market_week_path):
            os.makedirs(const.origin_data_market_week_path)
            print('mkdir %s' % const.origin_data_market_week_path)
        if not os.path.exists(const.origin_data_ref_path):
            os.makedirs(const.origin_data_ref_path)
            print('mkdir %s' % const.origin_data_ref_path)
        print('origin_data 目录检测完毕！')

    @utility.time_it
    def run_only_once(self):
        """
        下载初始化数据，注意顺序不要乱，初始化数据以 last_update_time 为准
        第一次执行，且只需运行一次即可
        """
        print('\n=====TushareFetch run_only_once start=====', end='\n')
        print('开始时间：%s' % datetime.now(), end='\n')
        # 判断 origin 目录下的上证指数文件是否存在，不存在则开始初始化下载
        filename = os.path.join(const.origin_data_index_day_path,
                                const.CODE_INDEX_SH + '.csv')
        if not os.path.exists(filename):
            if not self.debug:
                # 下载股票基础资料
                self.get_stock_basic()
                # 下载交易日历
                self.get_stock_trade_cal(self.last_update_time)
            # 下载股票日K线
            self.only_once_all_stock_data(self.last_update_time)
            # 下载沪深港通数据
            self.only_once_hsgt_data(self.last_update_time)
            # 一定最后下载指数数据
            self.only_once_stock_index_day(self.last_update_time)
        else:
            print('文件已经存在，无需在初始化下载，请直接运行每日更新任务！', end='\n')
        print('\n结束时间：%s' % datetime.now(), end='\n')
        print('=====TushareFetch run_only_once done!=====', end='\n')

    @utility.time_it
    def run_daily_job(self):
        """
        更新数据，注意顺序不要乱
        """
        print('\n=====TushareFetch run_daily_job start=====', end='\n')
        print('开始时间：%s' % datetime.now(), end='\n')
        if not self.debug:
            # 更新股票基础资料
            self.get_stock_basic()
            # 更新到当前日期的交易日历
            self.get_stock_trade_cal(self.today_date)
            time.sleep(1)
        # 计算从 last_update_time 到今天以来的需要更新日期
        date_list = self.get_update_duration()
        if len(date_list) > 0:
            for date in date_list:
                # 更新股票数据
                print('更新日期：%s' % date, end='\n')
                self.daily_job_stock_daily_by_date(date)
                # 下载沪深港通数据
                self.daily_job_hsgt_data(date)
                # 一定最后更新指数数据，以便下次更新时检测最后更新日期
                self.daily_job_index_daily_by_date(date)
            # 指数数据去重
            self.index_day_drop_duplicates()
            # 股票数据去重
            self.stock_day_drop_duplicates()
            # 沪深港通数据去重
            # self.hsgt_day_drop_duplicates()
            return True
        else:
            print('\n没有需要更新的数据', end='\n')
            return False
        print('\n结束时间：%s' % datetime.now(), end='\n')
        print('=====TushareFetch run_daily_job done!=====', end='\n')

    @utility.time_it
    def repeat_daily_job(self, trade_date):
        """
        重复执行某一天的日常任务，以便有错误或者漏下了数据后重新计算
        """
        print('\n=====TushareFetch repeat_daily_job start=====', end='\n')
        print('开始时间：%s' % datetime.now(), end='\n')
        trade_date = int(trade_date)
        # 更新股票数据
        print('更新日期：%s' % trade_date, end='\n')
        self.daily_job_stock_daily_by_date(trade_date)
        # 下载沪深港通数据
        self.daily_job_hsgt_data(trade_date)
        # 一定最后更新指数数据，以便下次更新时检测最后更新日期
        self.daily_job_index_daily_by_date(trade_date)
        # 指数数据去重
        self.index_day_drop_duplicates()
        # 股票数据去重
        self.stock_day_drop_duplicates()
        # 沪深港通数据去重
        # self.hsgt_day_drop_duplicates()
        print('\n结束时间：%s' % datetime.now(), end='\n')
        print('=====TushareFetch repeat_daily_job done!=====', end='\n')

    @utility.time_it
    def get_stock_basic(self):
        """
        获得上市状态的股票列表，只取6个字段
        """
        print('\n=====获得上市状态的股票列表开始=====', end='\n')
        print('开始时间：%s' % datetime.now(), end='\n')
        df = self.pro.stock_basic(
            list_status='L',
            fields='ts_code,symbol,name,area,industry,list_date')
        df.to_csv(
            const.ORIGIN_DATA_STOCK_BASIC,
            header=True,
            index=False,
            columns=[
                'ts_code', 'symbol', 'name', 'area', 'industry', 'list_date'
            ])
        print('文件：%s' % const.ORIGIN_DATA_STOCK_BASIC, end='\n')
        print('结束时间：%s' % datetime.now(), end='\n')
        print('=====获得上市状态的股票列表开始 done!=====', end='\n')

    @utility.time_it
    def get_stock_trade_cal(self, end_date=None):
        """
        获取从开市以来的交易日历
        """
        print('\n=====获取从开市以来的交易日历=====', end='\n')
        print('开始时间：%s' % datetime.now(), end='\n')
        if end_date is None:
            end_date = self.today_date
        df = self.pro.trade_cal(
            exchange='', start_date=const.START_DATE_SH, end_date=end_date)
        # print(df)
        # 降序排列
        df = df.sort_values(by=['cal_date'], ascending=False)
        # 写入交易日历文件
        df.to_csv(
            const.ORIGIN_DATA_STOCK_TRADE_CAL,
            header=True,
            index=False,
            columns=['exchange', 'cal_date', 'is_open'])
        # print('文件：%s' % const.ORIGIN_DATA_STOCK_TRADE_CAL)
        print('结束时间：%s' % datetime.now(), end='\n')
        print('=====获取从开市以来的交易日历 done!=====', end='\n')

    @utility.time_it
    def only_once_stock_index_day(self, end_date=None):
        """
        获得指数日线行情
        """
        print('\n=====获得指数日线行情=====', end='\n')
        print('开始时间：%s' % datetime.now(), end='\n')
        if end_date is None:
            end_date = self.today_date
        start_date = self.get_cal_start_date()
        for index in const.CODE_INDEX_LIST:
            df = self.pro.index_daily(
                ts_code=index, start_date=start_date, end_date=end_date)
            # print(df)
            filename = os.path.join(const.origin_data_index_day_path,
                                    index + '.csv')
            df.to_csv(
                filename, header=True, index=False, columns=const.COLUMNS)
            print('文件：%s' % filename, end='\r')
        print('\n结束时间：%s' % datetime.now(), end='\n')
        print('=====获得指数日线行情 done!=====', end='\n')

    @utility.time_it
    def only_once_all_stock_data(self, end_date):
        """
        获取全部股票的全部日线
        """
        print('\n=====拉取全部日线数据=====', end='\n')
        print('开始时间：%s' % datetime.now(), end='\n')
        if end_date is None:
            end_date = self.today_date
        if not self.debug:
            stock_list = pd.read_csv(const.ORIGIN_DATA_STOCK_BASIC)
        else:
            stock_list = pd.read_csv(const.DEBUG_DATA_STOCK_BASIC)
        count, j = 1, 1
        # 该接口每分钟最多调用200次，要控制
        max_sec, max_count = 60, 150
        start_time = datetime.utcnow()
        # print(start_time)
        for index, row in stock_list.iterrows():
            cur_time = datetime.utcnow()
            c = (cur_time - start_time)
            if c.seconds <= max_sec:
                if j >= max_count:
                    # 暂停
                    pause = max_sec - int(c.seconds)
                    for i in range(pause, -1, -1):
                        print('达到阈值，暂停剩余时间 %s 秒！' % str(i).zfill(2), end='\r')
                        time.sleep(1)
                    start_time = datetime.utcnow()
                    cur_time = datetime.utcnow()
                    c = (cur_time - start_time)
                    j = 1
                else:
                    j = j + 1
            else:
                start_time = datetime.utcnow()
                cur_time = datetime.utcnow()
                c = (cur_time - start_time)
                j = 1
            # print(row["ts_code"], row['symbol'], row['name'], row["list_date"])
            self.only_once_stock_daily_by_code(
                ts_code=row["ts_code"],
                start_date=row["list_date"],
                end_date=end_date)
            time.sleep(0.2)  # 接口访问之间暂停0.2秒
            print(
                'code: %s，第%s个/%s秒，共%s个，接口限制：%s 秒内 %s 个' %
                (row["ts_code"], j, c.seconds, count, max_sec, max_count),
                end='\r')
            count = count + 1
        print('\n结束时间：%s' % datetime.now(), end='\n')
        print('=====拉取全部日线数据 done!=====', end='\n')

    def only_once_stock_daily_by_code(self, ts_code, start_date, end_date):
        """
        获取单个股票日线行情，初始化全部股票数据时用
        """
        df = self.pro.daily(
            ts_code=ts_code, start_date=start_date, end_date=end_date)
        name = '%s.csv' % (ts_code)
        filename = os.path.join(const.origin_data_market_day_path, name)
        df.to_csv(filename, index=False, columns=const.COLUMNS)
        # print('%s 成功' % name, end='\r')
        # print('文件：%s' % filename)

    @utility.time_it
    def only_once_hsgt_data(self, end_date=None):
        """
        获取沪深港通的日数据
        """
        print('\n=====获取沪深港通的日数据=====', end='\n')
        print('开始时间：%s' % datetime.now(), end='\n')
        if end_date is None:
            end_date = self.today_date
        end_date_tmp = datetime.strptime(end_date, "%Y%m%d")
        filename = const.ORIGIN_DATA_MONEYFLOW_HSGT
        start_year = 2014  # 沪股通开始有数据年份为2014年，每年取一次数据，多次取
        # year = datetime.now().year
        hsgt = None
        while start_year <= end_date_tmp.year:
            start_date = '%s0101' % start_year
            if start_year == end_date_tmp.year:
                end_date = end_date_tmp.strftime("%Y%m%d")
                print(end_date)
            else:
                end_date = '%s1231' % start_year
            df = self.pro.moneyflow_hsgt(
                start_date=start_date, end_date=end_date)
            df = df.sort_values(by=['trade_date'], ascending=False)
            if hsgt is None:
                hsgt = df
            else:
                hsgt = hsgt.append(df)
            print('start_year: %s' % start_year, end='\n')
            start_year = start_year + 1
            # print(df)
        # 降序
        hsgt = hsgt.sort_values(by=['trade_date'], ascending=False)
        hsgt.to_csv(filename, index=False)
        print('\n结束时间：%s' % datetime.now(), end='\n')
        print('=====获取沪深港通的日数据 done!=====', end='\n')

    @utility.time_it
    def daily_job_hsgt_data(self, trade_date):
        """
        更新沪深港通的日数据
        """
        print('\n=====更新沪深港通的日数据=====', end='\n')
        print('开始时间：%s' % datetime.now(), end='\n')
        filename = const.ORIGIN_DATA_MONEYFLOW_HSGT
        df = pd.read_csv(filename)
        # 升序
        df = df.sort_values(by=['trade_date'])
        # print(df)
        # print(df.dtypes)
        # 检查是否存在该日期
        tmp_df = df[(df['trade_date'] == int(trade_date))]
        if (len(tmp_df) <= 0):  # 不存在则追加
            new_data = self.pro.query('moneyflow_hsgt', trade_date=trade_date)
            # print(new_data)
            if (len(new_data) > 0):
                new_data['trade_date'] = new_data['trade_date'].astype('int64')
                # print(new_data.dtypes)
                df = df.append(new_data)
                # print(df)
                # 降序
                df = df.sort_values(by=['trade_date'], ascending=False)
                # print(df)
                df.to_csv(filename, index=False)
        print('\n结束时间：%s' % datetime.now(), end='\n')
        print('=====更新沪深港通的日数据 done!=====', end='\n')

    @utility.time_it
    def daily_job_index_daily_by_date(self, trade_date=None):
        """
        更新指数日线行情
        """
        print('\n===== 更新指数日线行情 trade_date: %s =====' % trade_date, end='\n')
        print('开始时间：%s' % datetime.now(), end='\n')
        if trade_date is None:
            trade_date = self.today_date
        for li in const.CODE_INDEX_LIST:
            df = self.pro.index_daily(ts_code=li, trade_date=trade_date)
            # print(df)
            if (len(df) > 0):
                filename = os.path.join(const.origin_data_index_day_path,
                                        li + '.csv')
                # 存在文件追加数据行
                if os.path.exists(filename):
                    stock_data = pd.read_csv(filename)
                    # 检查是否存在该日期
                    tmp_df = stock_data[(
                        stock_data['trade_date'] == int(trade_date))]
                    if (len(tmp_df) <= 0):  # 不存在则追加
                        df.index = [len(stock_data) - 1]  # 索引值计算，不计算也没问题
                        # print(df)
                        stock_data = df.append(stock_data)
                        # print(stock_data)
                        stock_data.to_csv(
                            filename, index=False, columns=const.COLUMNS)
                        print('%s 数据更新成功' % li, end='\r')
                    # print('文件：%s' % filename)
                else:
                    # print('没有相关数据文件: ' % filename)
                    pass
            else:
                print('接口数据为空！', end='\n')
        print('\n结束时间：%s' % datetime.now(), end='\n')
        print('=====更新指数日线行情 done!=====', end='\n')

    @utility.time_it
    def daily_job_stock_daily_by_date(self, trade_date=None):
        """
        更新股票日线行情
        """
        print('\n===== 更新股票日线行情 trade_date: %s =====' % trade_date, end='\n')
        print('开始时间：%s' % datetime.now(), end='\n')
        if trade_date is None:
            trade_date = self.today_date
        df = self.pro.daily(trade_date=trade_date)
        if (len(df) > 0):
            for index, row in df.iterrows():
                name = '%s.csv' % (row['ts_code'])
                filename = os.path.join(const.origin_data_market_day_path,
                                        name)
                # 存在文件追加数据行
                if os.path.exists(filename):
                    stock_data = pd.read_csv(filename)
                    # 检查是否存在该日期
                    tmp_df = stock_data[(
                        stock_data['trade_date'] == int(trade_date))]
                    if (len(tmp_df) <= 0):  # 不存在则追加
                        copy_df_row = df.loc[[index]]  # 这里是dataframe
                        copy_df_row.index = [len(stock_data) - 1
                                             ]  # 索引值计算，不计算也没问题
                        # print(copy_df_row)
                        stock_data = copy_df_row.append(stock_data)
                        # print(stock_data)
                        stock_data.to_csv(
                            filename, index=False, columns=const.COLUMNS)
                        print('%s 数据更新成功' % row['ts_code'], end='\r')
                        # print('文件：%s' % filename)
                else:
                    # print('没有相关数据文件: ' % filename)
                    pass
                percent = round(1.00 * index / len(df) * 100, 2)
                print(
                    '进度 : %s [%d/%d]，code:%s' % (
                        (str(percent) + '%', index, len(df), row['ts_code'])),
                    end='\r')
        else:
            print('接口数据为空！', end='\n')
        print('\n结束时间：%s' % datetime.now(), end='\n')
        print('=====更新股票日线行情 done!=====', end='\n')

    @utility.time_it
    def index_day_drop_duplicates(self):
        """
        指数文件去重
        """
        print('\n=====指数文件去重=====', end='\n')
        print('开始时间：%s' % datetime.now(), end='\n')
        self.drop_duplicates(const.origin_data_index_day_path)
        print('\n结束时间：%s' % datetime.now(), end='\n')
        print('=====指数文件去重 done!=====', end='\n')

    @utility.time_it
    def stock_day_drop_duplicates(self):
        """
        股票文件去重
        """
        print('\n=====股票文件去重=====', end='\n')
        print('开始时间：%s' % datetime.now(), end='\n')
        self.drop_duplicates(const.origin_data_market_day_path)
        print('\n结束时间：%s' % datetime.now(), end='\n')
        print('=====股票文件去重 done!=====', end='\n')

    @utility.time_it
    def hsgt_day_drop_duplicates(self):
        """
        沪深港通文件去重
        """
        print('\n=====沪深港通文件去重=====', end='\n')
        print('开始时间：%s' % datetime.now(), end='\n')
        self.drop_duplicates(const.origin_data_ref_path)
        print('\n结束时间：%s' % datetime.now(), end='\n')
        print('=====沪深港通文件去重 done!=====', end='\n')

    def test(self):
        new_data = self.pro.query('moneyflow_hsgt', trade_date='20190327')
        print(new_data)
