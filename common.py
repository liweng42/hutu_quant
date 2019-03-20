import os
import stock_const as const
import pandas as pd
import numpy as np


class Common():
    '通用类，一些共用方法在这里面'
    pass


    def get_macd_data(df_raw,fast_period=12,slow_period=26,signal_period=9):
        # df_raw['close'] -- 收盘价
        # 收盘价按照日期升序( df_raw['date'] )排列
        # 返回值都是 Series
        fast_ewm=df_raw['close'].ewm(span=fast_period).mean()
        slow_ewm=df_raw['close'].ewm(span=slow_period).mean()

        dif=fast_ewm-slow_ewm
        dea=dif.ewm(span=signal_period).mean()
        # 一般概念里，macd柱是 (dif-dea)*2，实际上只是为了扩大显示效果
        # 实测后发现，也可以不乘以2，效果也足够清楚了
        bar=(dif-dea)*2
    # 将bar 分成红绿柱分别导出数据，
    #目的是后续用matplotlib绘图时，能够分色绘制
        red_bar=bar[bar>=0]
        green_bar=bar[bar<0]
        return dif,dea,red_bar,green_bar