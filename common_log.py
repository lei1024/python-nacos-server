import logging
from logging import handlers

"""
    获取日志处理对象
    :param filename: 日志文件名称
    :param level: 日志等级：debug, info, warn/warning, error, critical
    :param when: 日志文件分割的时间单位，单位有以下几种:<br>
          - S 秒<br>
          - M 分<br>
          - H 小时<br>
          - D 天<br>
          - W 每星期<br>
          - midnight 每天凌晨<br>
    :param backupCount: 备份文件的个数，如果超过这个数量，就会自动删除
    :param fmt: 日志信息格式
"""


# 日志级别字典
__level_dict = {
    'critical': logging.CRITICAL,
    'fatal': logging.CRITICAL,
    'error': logging.ERROR,
    'warning': logging.WARNING,
    'warn': logging.WARNING,
    'info': logging.INFO,
    'debug': logging.DEBUG
}

# 封装日志方法
def get_log(filename, level, when='MIDNIGHT', backupCount=3, maxBytes=10000000, fmt='%(asctime)s - %(pathname)s[line:%(lineno)d] - %(levelname)s: %(message)s'):
    level = __level_dict.get(level.lower(), None)
    logger = logging.getLogger(filename)  # 设置日志名称
    format_str = logging.Formatter(fmt)  # 设置日志格式
    logger.setLevel(level)  # 设置日志级别
    console_handler = logging.StreamHandler()  # 控制台输出
    console_handler.setFormatter(format_str)  # 控制台输出的格式
    logger.addHandler(console_handler)  # 控制台输出
    file_handler = handlers.RotatingFileHandler(filename=filename, maxBytes=maxBytes, backupCount=backupCount, encoding='utf-8')  # 文件输出
    file_handler.setFormatter(format_str) # 文件输出格式
    logger.addHandler(file_handler) # 日志输出
    return logger
