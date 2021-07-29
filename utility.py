import time
from log_manager import logger

def time_it(func):
    def timed(*args):
        start_time = time.time()
        result = func(*args)
        # 计算时间差值
        seconds, minutes, hours = int(time.time() - start_time), 0, 0
        hours = seconds // 3600
        minutes = (seconds - hours * 3600) // 60
        seconds = seconds - hours * 3600 - minutes * 60

        name = func.__name__
        arg_str = ', '.join(repr(arg) for arg in args)
        logger.info("{n}({a})".format(n=name, a=arg_str))
        logger.info("elapsed time: {:>02d}:{:>02d}:{:>02d}".format(
            hours, minutes, seconds))
        return result

    return timed


def str_to_bool(str):
    return True if str.lower() == 'true' else False


def trans_date_to_int(trade_date):
    """
    转换输入参数日期到整形，去掉日期间的横线-
    """
    pass


def trans_int_data_to_str(trade_date):
    """
    转换输入参数日期整型到日期形态的字符串，年月日用横线-分隔，传入参数日期一共有8位为整型
    """
    s = str(trade_date)
    year = s[:4]
    month = s[4:6]
    day = s[6:8]
    seq = (year, month, day)
    s = '-'.join(seq)
    return s
