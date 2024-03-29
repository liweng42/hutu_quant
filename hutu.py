#!/usr/bin/env python
# _*_ coding:utf-8 _*_

import os
import stock_const as const
import pandas as pd
from enum import Enum
from datetime import datetime
import time
import configparser
import codecs
import utility
from log_manager import logger


class Hutu:
    """这里是hutu 的抽象类，其他类应该继承自此类"""

    # 当天日期
    today_date = time.strftime('%Y%m%d', time.localtime(time.time()))
    # 上次更新日期，默认值
    last_update_time = '20050101'
    # 每天更新时间，17点整后开始更新
    begin_down_time = '17:00'

    hutu_type = {
        'origin':
        os.path.join(const.origin_data_index_day_path,
                     const.CODE_INDEX_SH + '.csv'),
        'process':
        os.path.join(const.process_data_index_day_path,
                     const.CODE_INDEX_SH + '.csv'),
        'emotion':
        os.path.join(const.emotion_index_data_root_path, 'emotion_basic.csv'),
        'select':
        os.path.join(const.select_data_root_path, 'select.csv')
    }

    def __init__(self):
        """
        初始化
        """
        # 这里初始化上次更新日期为 origin 目录下的上证指数文件最后更新日期
        self.last_update_time = self.get_last_update_time(
            self.hutu_type['origin'])
        self.init_config_value()

    def init_config_value(self):
        """
        初始化配置值，从配置文件读取
        """
        cp = configparser.RawConfigParser({
            # 设置配置文件的默认值
            const.CONFIG_APP_NAME: 'hutu_quant',
            const.CONFIG_LAST_UPDATE_TIME: self.last_update_time,
            const.CONFIG_DEBUG: True
        })
        config_file = os.path.join(os.getcwd(), const.CONFIG_FILE_NAME)
        if os.path.exists(config_file):
            with codecs.open(config_file, 'r', encoding='utf-8') as f:
                cp.readfp(f)
        self.app_name = self.read_from_config(cp, const.CONFIG_APP_NAME)
        self.debug = utility.str_to_bool(
            self.read_from_config(cp, const.CONFIG_DEBUG))
        # logger.info(self.debug)
        self.last_update_time = self.read_from_config(
            cp, const.CONFIG_LAST_UPDATE_TIME)

    def read_from_config(self, cp, item_name):
        """
        从配置文件读取配置节的值
        """
        if (cp.has_option(const.CONFIG_DEFAULT, item_name)):
            return cp.get(const.CONFIG_DEFAULT, item_name)

    def check_folder(self):
        """
        检测目录结构，不存在就创建
        """
        pass

    def run_only_once(self):
        """
        第一次且仅运行一次
        """
        pass

    def run_daily_job(self):
        """
        每日运行，作业
        """
        pass

    def get_last_update_time(self, filename):
        """
        根据源文件判断最后更新日期
        """
        if os.path.exists(filename):
            logger.info('文件：%s' % filename)
            df = pd.read_csv(filename)
            # logger.info(df)
            # 将数据按照交易日期从近到远排序
            df = df.sort_values(by=['trade_date'], ascending=False)
            df = df[0:1]  # 取第一行数据
            return df['trade_date'].values[0]  # 取trade_date列值
            # logger.info(self.last_update_time)
        else:
            return self.last_update_time

    def get_update_duration(self):
        """
        计算从 last_update_time 到今天以来的需要更新日期
        """
        if self.debug:
            df = pd.read_csv(const.DEBUG_DATA_STOCK_TRADE_CAL)
        else:
            df = pd.read_csv(const.ORIGIN_DATA_STOCK_TRADE_CAL)
        df = df[(df['cal_date'] > int(self.last_update_time))
                & (df['is_open'] > 0)]
        # logger.info(df)
        list = []
        for index, row in df.iterrows():
            list.append(row['cal_date'])
        list.sort()
        # 判断当前时间，如果在begin_down_time后，可以计算当天日期，否则排除当天日期
        d1 = datetime.strptime(
            str(datetime.now().date()) + self.begin_down_time, '%Y-%m-%d%H:%M')
        if datetime.now() < d1 and int(self.today_date) in list:
            list.remove(int(self.today_date))
        # logger.info(list)
        logger.info('上次更新日期为：%s， 需要更新的日期有：%s' % (self.last_update_time, list))
        return list

    def get_cal_start_date(self):
        """
        获取交易日历文件的起始日期，从2005-01-01开始即可
        """
        if self.debug:
            df = pd.read_csv(const.DEBUG_DATA_STOCK_TRADE_CAL)
        else:
            df = pd.read_csv(const.ORIGIN_DATA_STOCK_TRADE_CAL)
        # logger.info(self.debug)
        df = df.sort_values(by=['cal_date'])
        df = df[(df['cal_date'] > 20050101) & (df['is_open'] > 0)]
        start_date = df['cal_date'].values[0]
        logger.debug(start_date)
        return str(start_date)

    def get_cal_end_date(self):
        """
        获取交易日历文件的结束日期
        """
        if self.debug:
            df = pd.read_csv(const.DEBUG_DATA_STOCK_TRADE_CAL)
        else:
            df = pd.read_csv(const.ORIGIN_DATA_STOCK_TRADE_CAL)
        # logger.info(self.debug)
        df = df[(df['is_open'] > 0)]
        end_date = df['cal_date'].values[0]
        logger.info(end_date)
        return str(end_date)

    def get_cal_open_list(self):
        """
        获得开市的交易日期list，从2005-01-01开始即可
        """
        if self.debug:
            df = pd.read_csv(const.DEBUG_DATA_STOCK_TRADE_CAL)
        else:
            df = pd.read_csv(const.ORIGIN_DATA_STOCK_TRADE_CAL)
        # logger.info(self.debug)
        df = df[(df['cal_date'] > 20050101) & (df['is_open'] > 0)]
        list = []
        for index, row in df.iterrows():
            list.append(row['cal_date'])
        list.sort()
        return list

    def drop_duplicates(self, path):
        """
        数据去重，依据文件中的 trade_date列
        """
        for root, dirs, files in os.walk(path):
            files.sort()
            count = 1
            for file in files:
                if os.path.splitext(file)[1] == '.csv':
                    filename = os.path.join(path, file)
                    percent = round(1.00 * count / len(files) * 100, 2)
                    logger.info(
                        '进度 : %s [%d/%d]，file:%s' % (
                            (str(percent) + '%', count, len(files), file)),
                        )
                    stock_data = pd.read_csv(filename)
                    # 去重
                    stock_data.drop_duplicates('trade_date', inplace=True)
                    stock_data.to_csv(
                        filename, index=False, columns=const.COLUMNS)
                count = count + 1
