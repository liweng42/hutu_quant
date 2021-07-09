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


# tushare 要求的api token
api_tokens = ([
    '0b295352f360d571aabad80f7d8980094306e6d03e6e3f4f55a8b29b'
    ])
for token in api_tokens:
    ts.set_token(token)
    pro = ts.pro_api()

    df = pro.daily(trade_date='20200325')
    print(token)
    print(df)
    time.sleep(3)  # 接口访问之间暂停 3 秒
