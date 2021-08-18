from hutu import Hutu
import tushare as ts
import stock_const as const
import time
from datetime import datetime
import os
import pandas as pd
import utility
import requests
from lxml import etree
from log_manager import logger

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
            logger.info('mkdir %s' % const.origin_data_basic_path)
        if not os.path.exists(const.origin_data_fin_path):
            os.makedirs(const.origin_data_fin_path)
            logger.info('mkdir %s' % const.origin_data_fin_path)
        if not os.path.exists(const.origin_data_index_day_path):
            os.makedirs(const.origin_data_index_day_path)
            logger.info('mkdir %s' % const.origin_data_index_day_path)
        if not os.path.exists(const.origin_data_index_week_path):
            os.makedirs(const.origin_data_index_week_path)
            logger.info('mkdir %s' % const.origin_data_index_week_path)
        if not os.path.exists(const.origin_data_market_day_path):
            os.makedirs(const.origin_data_market_day_path)
            logger.info('mkdir %s' % const.origin_data_market_day_path)
        if not os.path.exists(const.origin_data_market_week_path):
            os.makedirs(const.origin_data_market_week_path)
            logger.info('mkdir %s' % const.origin_data_market_week_path)
        if not os.path.exists(const.origin_data_ref_path):
            os.makedirs(const.origin_data_ref_path)
            logger.info('mkdir %s' % const.origin_data_ref_path)
        logger.info('origin_data 目录检测完毕！')

    def run_only_once(self):
        """
        下载初始化数据，注意顺序不要乱，初始化数据以 last_update_time 为准
        第一次执行，且只需运行一次即可
        """
        logger.info('=====TushareFetch run_only_once start=====')
        logger.info('开始时间：%s' % datetime.now())
        # 判断 origin 目录下的上证指数文件是否存在，不存在则开始初始化下载
        filename = os.path.join(const.origin_data_index_day_path,
                                const.CODE_INDEX_SH + '.csv')
        if not os.path.exists(filename):
            if not self.debug:
                # 下载股票基础资料
                self.get_stock_basic()
                # 下载交易日历，直接下载从 20050101 到 当前日期
                self.get_stock_trade_cal()
            # 下载股票日K线
            self.only_once_all_stock_data()
            # 下载沪深港通数据
            self.only_once_hsgt_data()
            # 一定最后下载指数数据
            self.only_once_stock_index_day()
        else:
            logger.info('文件已经存在，无需在初始化下载，请直接运行每日更新任务！')
        logger.info('结束时间：%s' % datetime.now())
        logger.info('=====TushareFetch run_only_once done!=====')

    @utility.time_it
    def run_daily_job(self):
        """
        更新数据，注意顺序不要乱
        """
        logger.info('=====TushareFetch run_daily_job start=====')
        logger.info('开始时间：%s' % datetime.now())
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
                logger.info('更新日期：%s' % date)
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
            logger.info('结束时间：%s' % datetime.now())
            logger.info('=====TushareFetch run_daily_job done!=====')           
            return True
        else:
            logger.info('没有需要更新的数据')
            logger.info('结束时间：%s' % datetime.now())
            logger.info('=====TushareFetch run_daily_job done!=====')              
            return False


    @utility.time_it
    def repeat_daily_job(self, trade_date):
        """
        重复执行某一天的日常任务，以便有错误或者漏下了数据后重新计算
        """
        logger.info('=====TushareFetch repeat_daily_job start=====')
        logger.info('开始时间：%s' % datetime.now())
        trade_date = int(trade_date)
        # 更新股票数据
        logger.info('更新日期：%s' % trade_date)
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
        logger.info('结束时间：%s' % datetime.now())
        logger.info('=====TushareFetch repeat_daily_job done!=====')

    def get_stock_basic(self):
        """
        获得上市状态的股票列表，只取6个字段
        """
        logger.info('=====获得上市状态的股票列表开始=====')
        logger.info('开始时间：%s' % datetime.now())
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
        logger.info('文件：%s' % const.ORIGIN_DATA_STOCK_BASIC)
        logger.info('结束时间：%s' % datetime.now())
        logger.info('=====获得上市状态的股票列表开始 done!=====')

    def get_stock_trade_cal(self, end_date=None):
        """
        获取从开市以来的交易日历
        """
        logger.info('=====获取从开市以来的交易日历=====')
        logger.info('开始时间：%s' % datetime.now())
        if end_date is None:
            end_date = self.today_date
        df = self.pro.trade_cal(
            exchange='', start_date=const.START_DATE_SH, end_date=end_date)
        # logger.info(df)
        # 降序排列
        df = df.sort_values(by=['cal_date'], ascending=False)
        # 写入交易日历文件
        df.to_csv(
            const.ORIGIN_DATA_STOCK_TRADE_CAL,
            header=True,
            index=False,
            columns=['exchange', 'cal_date', 'is_open'])
        # logger.info('文件：%s' % const.ORIGIN_DATA_STOCK_TRADE_CAL)
        logger.info('结束时间：%s' % datetime.now())
        logger.info('=====获取从开市以来的交易日历 done!=====')

    def only_once_stock_index_day(self, end_date=None):
        """
        获得指数日线行情
        """
        logger.info('=====获得指数日线行情=====')
        logger.info('开始时间：%s' % datetime.now())
        if end_date is None:
            end_date = self.today_date
        start_date = self.get_cal_start_date()
        for index in const.CODE_INDEX_LIST:
            df = self.pro.index_daily(
                ts_code=index, start_date=start_date, end_date=end_date)
            # logger.info(df)
            filename = os.path.join(const.origin_data_index_day_path,
                                    index + '.csv')
            df.to_csv(
                filename, header=True, index=False, columns=const.COLUMNS)
            logger.info('文件：%s' % filename)
        logger.info('结束时间：%s' % datetime.now())
        logger.info('=====获得指数日线行情 done!=====')

    def only_once_all_stock_data(self, end_date=None):
        """
        获取全部股票的全部日线
        """
        logger.info('=====拉取全部日线数据=====')
        logger.info('开始时间：%s' % datetime.now())
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
        # logger.info(start_time)
        for index, row in stock_list.iterrows():
            cur_time = datetime.utcnow()
            c = (cur_time - start_time)
            if c.seconds <= max_sec:
                if j >= max_count:
                    # 暂停
                    pause = max_sec - int(c.seconds)
                    for i in range(pause, -1, -1):
                        logger.info('达到阈值，暂停剩余时间 %s 秒！' % str(i).zfill(2))
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
            # logger.info(row["ts_code"], row['symbol'], row['name'], row["list_date"])
            self.only_once_stock_daily_by_code(
                ts_code=row["ts_code"],
                start_date=row["list_date"],
                end_date=end_date)
            time.sleep(0.2)  # 接口访问之间暂停0.2秒
            logger.info(
                'code: %s，第%s个/%s秒，共%s个，接口限制：%s 秒内 %s 个' %
                (row["ts_code"], j, c.seconds, count, max_sec, max_count),
                )
            count = count + 1
        logger.info('结束时间：%s' % datetime.now())
        logger.info('=====拉取全部日线数据 done!=====')

    def only_once_stock_daily_by_code(self, ts_code, start_date, end_date):
        """
        获取单个股票日线行情，初始化股票数据时用
        """
        df = self.pro.daily(
            ts_code=ts_code, start_date=start_date, end_date=end_date)
        name = '%s.csv' % (ts_code)
        filename = os.path.join(const.origin_data_market_day_path, name)
        df.to_csv(filename, index=False, columns=const.COLUMNS)
        logger.info('%s 成功' % name)
        logger.info('文件：%s' % filename)


    def only_once_hsgt_data(self, end_date=None):
        """
        获取沪深港通的日数据
        """
        logger.info('=====获取沪深港通的日数据=====')
        logger.info('开始时间：%s' % datetime.now())
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
                logger.info(end_date)
            else:
                end_date = '%s1231' % start_year
            df = self.pro.moneyflow_hsgt(
                start_date=start_date, end_date=end_date)
            df = df.sort_values(by=['trade_date'], ascending=False)
            if hsgt is None:
                hsgt = df
            else:
                hsgt = hsgt.append(df)
            logger.info('start_year: %s' % start_year)
            start_year = start_year + 1
            # logger.info(df)
        # 降序
        if hsgt is not None:
            hsgt = hsgt.sort_values(by=['trade_date'], ascending=False)
            hsgt.to_csv(filename, index=False)
            logger.info('沪深港通数据获取成功！%s' % datetime.now())
        else:
            logger.info('沪深港通数据获取失败！%s' % datetime.now())
        logger.info('结束时间：%s' % datetime.now())
        logger.info('=====获取沪深港通的日数据 done!=====')

    @utility.time_it
    def daily_job_hsgt_data(self, trade_date):
        """
        更新沪深港通的日数据
        """
        logger.info('=====更新沪深港通的日数据=====')
        logger.info('开始时间：%s' % datetime.now())
        filename = const.ORIGIN_DATA_MONEYFLOW_HSGT
        df = pd.read_csv(filename)
        # 升序
        df = df.sort_values(by=['trade_date'])
        # logger.info(df)
        # logger.info(df.dtypes)
        # 检查是否存在该日期
        tmp_df = df[(df['trade_date'] == int(trade_date))]
        if (len(tmp_df) <= 0):  # 不存在则追加
            # 暂时不从 tushare 取hsgt数据，该从同花顺抓取，tushare 这部分日数据更新不及时
            # new_data = self.pro.query('moneyflow_hsgt', trade_date=trade_date)
            # logger.info(new_data)
            # if (len(new_data) > 0):
            #     new_data['trade_date'] = new_data['trade_date'].astype('int64')
            #     # logger.info(new_data.dtypes)
            #     df = df.append(new_data)
            #     # logger.info(df)
            #     # 降序
            #     df = df.sort_values(by=['trade_date'], ascending=False)
            #     # logger.info(df)
            #     df.to_csv(filename, index=False)
            date1 = utility.trans_int_data_to_str(trade_date)
            money_data = self.get_hsgt_data_by_10jqka(date1)
            north_money = round(money_data['hgt'] + money_data['sgt'], 2)
            new_data = [
                int(trade_date), 0, 0, money_data['hgt'], money_data['sgt'],
                north_money, 0
            ]
            new_df = pd.DataFrame(columns=[
                'trade_date', 'ggt_ss', 'ggt_sz', 'hgt', 'sgt', 'north_money',
                'south_money'
            ])
            # new_data['trade_date'] = new_data['trade_date'].astype('int64')
            # 插入数据（忽略索引）
            new_df.loc[0] = new_data
            logger.info(new_df)
            # logger.info(type(new_df['trade_date']))
            df = df.append(new_df)
            # 降序
            df = df.sort_values(by=['trade_date'], ascending=False)
            # logger.info(type(df['trade_date']))
            logger.info(df)
            df.to_csv(filename, index=False)
        logger.info('结束时间：%s' % datetime.now())
        logger.info('=====更新沪深港通的日数据 done!=====')

    def get_hsgt_data_by_10jqka(self, trade_date):
        """
        从同花顺下载 trade_date 日期的沪深港通日数据，注意 trade_date只能是 最近10天之内，因为页面上最多10条
        """
        urls = {
            # 沪股通
            'hgt': 'http://data.10jqka.com.cn/hgt/hgtb/',
            # 深股通
            'sgt': 'http://data.10jqka.com.cn/hgt/sgtb/',
            # # 港股通(沪)
            # 'ggt_ss': 'http://data.10jqka.com.cn/hgt/ggtb/',
            # # 港股通(深)
            # 'ggt_sz': 'http://data.10jqka.com.cn/hgt/ggtbs/'
        }
        money_data = {
            # 沪股通
            'hgt': 0,
            # 深股通
            'sgt': 0,
            # # 港股通(沪)
            # 'ggt_ss': 0,
            # # 港股通(深)
            # 'ggt_sz': 0
        }
        logger.info(money_data)
        # 北向资金
        # north_money = 0
        # 南向资金
        # south_money = 0
        headers = {
            'Accept': 'text/html, */*; q=0.01',
            'Accept-Language': 'zh-cn',
            'Host': 'data.10jqka.com.cn',
            'User-Agent':
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/12.0 Safari/605.1.15',
            'Referer': 'http://data.10jqka.com.cn/hgt/hgtb/',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
        }
        for key in urls.keys():
            r = requests.get(urls[key], headers=headers, timeout=5)
            # logger.info(r.text)
            html = etree.HTML(r.text)
            trs = html.xpath('//*[@id="table1"]/table/tbody/tr')
            for tr in trs:
                if (tr[0].text == trade_date):
                    # logger.info(tr[1].text)
                    s = tr[1].text.replace('亿', '')
                    money_data[key] = round(float(s) * 100, 2)
                    break
        logger.info(money_data)
        return money_data

    @utility.time_it
    def daily_job_index_daily_by_date(self, trade_date=None):
        """
        更新指数日线行情
        """
        logger.info('===== 更新指数日线行情 trade_date: %s =====' % trade_date)
        logger.info('开始时间：%s' % datetime.now())
        if trade_date is None:
            trade_date = self.today_date
        for li in const.CODE_INDEX_LIST:
            df = self.pro.index_daily(ts_code=li, trade_date=trade_date)
            # logger.info(df)
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
                        # logger.info(df)
                        stock_data = df.append(stock_data)
                        # logger.info(stock_data)
                        stock_data.to_csv(
                            filename, index=False, columns=const.COLUMNS)
                        logger.info('%s 数据更新成功' % li)
                    # logger.info('文件：%s' % filename)
                else:
                    # logger.info('没有相关数据文件: ' % filename)
                    pass
            else:
                logger.info('接口数据为空！')
        logger.info('结束时间：%s' % datetime.now())
        logger.info('=====更新指数日线行情 done!=====')

    @utility.time_it
    def daily_job_stock_daily_by_date(self, trade_date=None):
        """
        更新股票日线行情
        """
        logger.info('===== 更新股票日线行情 trade_date: %s =====' % trade_date)
        logger.info('开始时间：%s' % datetime.now())
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
                    # logger.info('debug ----> %s' % filename)
                    stock_data = pd.read_csv(filename)
                    # 检查是否存在该日期
                    tmp_df = stock_data[(
                        stock_data['trade_date'] == int(trade_date))]
                    if (len(tmp_df) <= 0):  # 不存在则追加
                        copy_df_row = df.loc[[index]]  # 这里是dataframe
                        copy_df_row.index = [len(stock_data) - 1
                                             ]  # 索引值计算，不计算也没问题
                        # logger.info(copy_df_row)
                        stock_data = copy_df_row.append(stock_data)
                        # logger.info(stock_data)
                        stock_data.to_csv(
                            filename, index=False, columns=const.COLUMNS)
                        logger.info('%s 数据更新成功' % row['ts_code'])
                        # logger.info('文件：%s' % filename)
                # 不存在该日线文件，则一般为新上市股票，需要新下载日线文件
                else:
                    logger.info('没有日线数据文件: %s' % filename)
                    logger.info('下载 %s 日线数据文件' % row['ts_code'])
                    self.down_single_stock_day_data(row['ts_code'])
                percent = round(1.00 * index / len(df) * 100, 2)
                logger.info(
                    '进度 : %s [%d/%d]，code:%s' % (
                        (str(percent) + '%', index, len(df), row['ts_code'])),
                    )
        else:
            logger.info('接口数据为空！')
        logger.info('结束时间：%s' % datetime.now())
        logger.info('=====更新股票日线行情 done!=====')

    def down_single_stock_day_data(self, ts_code):
        """
        下载单个股票从上市以来的全部日线数据
        """
        list_date = self.get_single_stock_list_date(ts_code)
        self.only_once_stock_daily_by_code(
            ts_code=ts_code, start_date=list_date, end_date=self.today_date)

    def get_single_stock_list_date(self, ts_code):
        """
        获取单个股票的上市日期
        """
        if not self.debug:
            df = pd.read_csv(const.ORIGIN_DATA_STOCK_BASIC)
        else:
            df = pd.read_csv(const.DEBUG_DATA_STOCK_BASIC)
        # 查找单行记录
        df = df[df['ts_code'].str.match(ts_code)]
        if (len(df) > 0):
            return str(df['list_date'].values[0])
        else:
            return '20050101'

    @utility.time_it
    def index_day_drop_duplicates(self):
        """
        指数文件去重
        """
        logger.info('=====指数文件去重=====')
        logger.info('开始时间：%s' % datetime.now())
        self.drop_duplicates(const.origin_data_index_day_path)
        logger.info('结束时间：%s' % datetime.now())
        logger.info('=====指数文件去重 done!=====')

    @utility.time_it
    def stock_day_drop_duplicates(self):
        """
        股票文件去重
        """
        logger.info('=====股票文件去重=====')
        logger.info('开始时间：%s' % datetime.now())
        self.drop_duplicates(const.origin_data_market_day_path)
        logger.info('结束时间：%s' % datetime.now())
        logger.info('=====股票文件去重 done!=====')

    @utility.time_it
    def hsgt_day_drop_duplicates(self):
        """
        沪深港通文件去重
        """
        logger.info('=====沪深港通文件去重=====')
        logger.info('开始时间：%s' % datetime.now())
        self.drop_duplicates(const.origin_data_ref_path)
        logger.info('结束时间：%s' % datetime.now())
        logger.info('=====沪深港通文件去重 done!=====')

    def check_hsgt_data(self, trade_date):
        """
        检测沪深港通某日数据是否存在
        """
        new_data = self.pro.query('moneyflow_hsgt', trade_date=trade_date)
        logger.info(new_data)

    def test(self, trade_date):
        new_data = self.pro.query('moneyflow_hsgt', trade_date=trade_date)
        logger.info(new_data)

    def test2(self):
        self.down_single_stock_day_data('002700.SZ')
        # logger.info(a)
