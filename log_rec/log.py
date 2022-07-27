# encoding = utf-8
###
# @ Description: 日志封装文件
# @ Author: fatih
# @ Date: 2020-12-30 10:48:00
# @ FilePath: \mechineInfo\utils\log.py
# @ LastEditors: fatih
# @ LastEditTime: 2021-01-11 16:18:30
###
import logging


# 既把日志输出到控制台， 还要写入日志文件
class Logger:
    def __init__(self, logname="info", chlevel=logging.WARN, loggername=None):
        """
           指定保存日志的文件路径，日志级别，以及调用文件
           将日志存入到指定的文件中
        """
        # print(loggername)
        # 创建一个logger
        self.logger = logging.getLogger(None)
        self.logger.setLevel(logging.INFO)
        # 创建一个handler，用于写入日志文件
        fh = logging.FileHandler(logname, mode='a')
        fh.setLevel(logging.INFO)
        if not self.logger.handlers:
            # 或者使用如下语句判断
            # if not self.logger.hasHandlers():

            # 再创建一个handler，用于输出到控制台
            ch = logging.StreamHandler()
            ch.setLevel(logging.WARN)
            # 定义handler的输出格式
            # formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            # %(asctime)s - %(pathname)s[line:%(lineno)d] - %(levelname)s: %(message)s
            formatter = logging.Formatter('[%(levelname)s]%(asctime)s [%(filename)s:%(lineno)d]: %(message)s')
            fh.setFormatter(formatter)
            fh.setLevel(logging.INFO)
            ch.setFormatter(formatter)
            # 给logger添加handler
            self.logger.addHandler(fh)
            self.logger.addHandler(ch)

        #     self.logger.fatal("add handler")
        # self.logger.fatal("set logger")

    def getlog(self):
        # self.logger.fatal("get logger")
        return self.logger
