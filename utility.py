import time


def time_it(func):
    def timed(*args):
        start_time = time.time()
        result = func(*args)
        # 计算时间差值
        seconds, minutes, hours = int(time.time() - start_time), 0, 0
        hours = seconds // 3600
        minutes = (seconds - hours*3600) // 60
        seconds = seconds - hours*3600 - minutes*60

        name = func.__name__
        arg_str = ', '.join(repr(arg) for arg in args)
        print("{n}({a})".format(n=name, a=arg_str))
        print("elapsed time: {:>02d}:{:>02d}:{:>02d}".format(hours, minutes, seconds))
        return result
    return timed


def str_to_bool(str):
    return True if str.lower() == 'true' else False
