import logging
import logging.handlers
import datetime
import os


class LogManager():
    '日志类'
    logger = logging.getLogger('mylogger')

    def __init__(self):
        log_path = os.path.join(os.getcwd(), 'log')
        if not os.path.exists(log_path):
            os.makedirs(log_path)
        # 往屏幕上输出
        sh_handler = logging.StreamHandler()
        self.logger.setLevel(logging.DEBUG)
        # 设置屏幕上显示的格式
        sh_handler.setFormatter(
            logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))

        # rf_handler = logging.handlers.TimedRotatingFileHandler(
        #     './log/log.log',
        #     when='D',
        #     interval=1,
        #     backupCount=7,
        #     atTime=datetime.time(0, 0, 0, 0))
        # rf_handler.setLevel(logging.INFO)
        # rf_handler.setFormatter(
        #     logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))

        log_filename = '%d-%d-%d' % (datetime.datetime.now().year, datetime.datetime.now().month, datetime.datetime.now().day)
        f_handler = logging.FileHandler('./log/%s.log' % log_filename)
        f_handler.setLevel(logging.INFO)
        f_handler.setFormatter(
            logging.Formatter(
                "%(asctime)s - %(levelname)s - %(filename)s[:%(lineno)d] - %(message)s"
            ))

        error_f_handler = logging.FileHandler('./log/error.log')
        error_f_handler.setLevel(logging.ERROR)
        error_f_handler.setFormatter(
            logging.Formatter(
                "%(asctime)s - %(levelname)s - %(filename)s[:%(lineno)d] - %(message)s"
            ))
        self.logger.addHandler(sh_handler)
        self.logger.addHandler(f_handler)
        self.logger.addHandler(error_f_handler)

    def debug(self, msg):
        self.logger.debug(msg)

    def info(self, msg):
        self.logger.info(msg)

    def warning(self, msg):
        self.logger.warning(msg)

    def error(self, msg):
        self.logger.error(msg)

    def critical(self, msg):
        self.logger.critical(msg)


logger = LogManager()
# logger.debug('debug message')
# logger.info('info message')
# logger.warning('warning message')
# logger.error('error message')
# logger.critical('critical message')
