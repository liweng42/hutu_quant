import os

# 上证指数代码
CODE_INDEX_SH = '000001.SH'
# 沪深300指数代码
CODE_INDEX_300 = '000300.SH'
# 深成指代码
CODE_INDEX_SZ = '399001.SZ'
# 创业板指代码
CODE_INDEX_399006 = '399006.SZ'

# 指数代码List
CODE_INDEX_LIST = [
    CODE_INDEX_300,
    CODE_INDEX_SZ,
    CODE_INDEX_399006,
    # 一定最后一个是上证指数，方便后面更新检测最后更新日期
    CODE_INDEX_SH
]

# 上证指数开市时间
START_DATE_SH = '19901219'

# CONFIG 文件配置节名称
CONFIG_FILE_NAME = 'config.conf'
CONFIG_DEFAULT = 'default'
# CONFIG 文件配置项名称
CONFIG_APP_NAME = 'APP_NAME'
# debug模式 Ture/False
CONFIG_DEBUG = 'DEBUG'
CONFIG_LAST_UPDATE_TIME = 'LAST_UPDATE_TIME'


# 常用数据列
COLUMNS = ['ts_code', 'trade_date', 'open', 'high', 'low', 'close', 'pre_close', 'change', 'pct_chg', 'vol', 'amount']
# 指标列
INDICATOR_COLUMNS = ['ma5', 'ma20', 'ma60', 'ma120', 'ma250', 'ema17', 'ema24', 'ema50', 'diff', 'dea', 'macd', 'rsi1']
# 情绪指标基本列
EMOTION_BASIC_COLUMNS = ['trade_date', 'close', 'pct_chg', 'vol', 'ema24', 'ma120', 'ma250', 'rise', 'fall', 'rise_limit', 'fall_limit', 'rise_limit_count', 'ema24_up', 'ma120_up']
# 情绪指标列
EMOTION_INDEX_COLUMNS = ['v', 'v_5']


# 原始数据根目录
origin_data_root_path = os.path.join(os.getcwd(), 'origin_data')
# 股票基础数据目录
origin_data_basic_path = os.path.join(origin_data_root_path, 'basic')
# 股票基础数据文件名
ORIGIN_DATA_STOCK_BASIC = os.path.join(origin_data_basic_path, 'stock_basic.csv')
# 股市交易日历文件名
ORIGIN_DATA_STOCK_TRADE_CAL = os.path.join(origin_data_basic_path, 'stock_trade_cal.csv')
# 财务数据目录
origin_data_fin_path = os.path.join(origin_data_root_path, 'fin')
# 指数日K线文件目录
origin_data_index_day_path = os.path.join(origin_data_root_path, 'index/day')
# 指数周K线文件目录
origin_data_index_week_path = os.path.join(origin_data_root_path, 'index/week')
# 行情数据 -- 日K线文件目录
origin_data_market_day_path = os.path.join(origin_data_root_path, 'market/day')
# 行情数据 -- 周K线文件目录
origin_data_market_week_path = os.path.join(origin_data_root_path, 'market/week')
# 市场参考数据目录
origin_data_ref_path = os.path.join(origin_data_root_path, 'ref')
# 沪深港通数据文件
ORIGIN_DATA_MONEYFLOW_HSGT = os.path.join(origin_data_ref_path, 'moneyflow_hsgt.csv')

# 处理后数据根目录
process_data_root_path = os.path.join(os.getcwd(), 'process_data')
# 股票基础数据目录
process_data_basic_path = os.path.join(process_data_root_path, 'basic')
# 财务数据目录
process_data_fin_path = os.path.join(process_data_root_path, 'fin')
# 指数日K线文件目录
process_data_index_day_path = os.path.join(process_data_root_path, 'index/day')
# 指数周K线文件目录
process_data_index_week_path = os.path.join(process_data_root_path, 'index/week')
# 行情数据 -- 日K线文件目录
process_data_market_day_path = os.path.join(process_data_root_path, 'market/day')
# 行情数据 -- 周K线文件目录
process_data_market_week_path = os.path.join(process_data_root_path, 'market/week')
# 市场参考数据目录
process_data_ref_path = os.path.join(process_data_root_path, 'ref')

# 市场情绪指标根目录
emotion_index_data_root_path = os.path.join(os.getcwd(), 'emotion_index')
# 情绪指标中间数据目录
emotion_index_data_day_path = os.path.join(emotion_index_data_root_path, 'day')

# Debug 数据文件目录
debug_data_root_path = os.path.join(os.getcwd(), 'debug_data')
# debug 股票基础数据文件名
DEBUG_DATA_STOCK_BASIC = os.path.join(debug_data_root_path, 'stock_basic.csv')
# Debug 股市交易日历文件名
DEBUG_DATA_STOCK_TRADE_CAL = os.path.join(debug_data_root_path, 'stock_trade_cal.csv')

# sample 数据文件目录
sample_data_root_path = os.path.join(os.getcwd(), 'sample_data')
# emotion_basic_sample 文件
SAMPLE_EMOTION_BASIC = os.path.join(sample_data_root_path, 'emotion_basic_sample.csv')
