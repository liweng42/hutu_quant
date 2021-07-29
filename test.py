from hutu import Hutu
from tushare_fetch import TushareFetch
import sys
import requests
from lxml import etree
import utility
from log_manager import logger


class MyTest(Hutu):
    def a(self):
        self.last_update_time = 20190315
        logger.info(self.last_update_time)

    def down_hsgt(self, trade_date):
        t = TushareFetch()        
        t.daily_job_hsgt_data(trade_date)

    def ths_hsgt(self):
        headers = {
            'Accept': 'text/html, */*; q=0.01',
            'Accept-Language': 'zh-cn',
            'Host': 'data.10jqka.com.cn',
            'User-Agent':
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/12.0 Safari/605.1.15',
            'Referer': 'http://data.10jqka.com.cn/hgt/sgtb/',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'X-Requested-With': 'XMLHttpRequest',
            # 'hexin-v': 'ApHQejnwqQQd9MUQ6094oau_pp8q_gTkr3CplHMmj-51NbvJu04VQD_CuVcA'
        }
        r = requests.get(
            'http://data.10jqka.com.cn/hgt/sgtb/', headers=headers, timeout=3)
        # logger.info(r.text)
        html = etree.HTML(r.text)
        trs = html.xpath('//*[@id="table1"]/table/tbody/tr')
        for tr in trs:
            logger.info(tr[0].text)
            # for td in tr:
            #     logger.info(td.text)

    def daily_job_hsgt_data_by_10jqka(self, trade_date):
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

    def test1(self):
        t = utility.trans_int_data_to_str(20190326)
        logger.info(t)


# trade_date = sys.argv[1]
# test = MyTest()
# test.daily_job_hsgt_data_by_10jqka('2019-03-26')
# test.down_hsgt(trade_date)
# test.test1()
# logger.info('test')

t = TushareFetch()
t.test2()
logger.info('t.test2()')
