import time

from mysql_tool import MysqlDao
from tushare_spider.settings import *
import tushare as ts
import pandas as pd
import hashlib
import threading


class TushareSpider:
    # 初始化参数
    def __init__(self):
        # 参数设置
        self.TUSHARE_VIPAPI = TUSHARE_VIPAPI  # API列表

        # 爬取状态
        self.TRACK_DATE = None
        self.TRACK_BOARD = None

        # 实例化Tushare对象
        self.TuShare = ts.pro_api('56a12424870cd0953907cde2c660b498c8fe774145b7f17afdc746dd')

        # 实例化数据库操作对象
        self.SqlObj = MysqlDao()

        # 保存的查询结果
        self.df_ts = None

    # 获取财务数据
    def get_financial_data(self, period):
        # 存储已经爬取过的date
        def save_date():
            df_date = pd.DataFrame(data=[self.TRACK_DATE], columns=['date'])
            self.SqlObj.insert_table('finished_date', df_date, {'PK': 'date', 'date': 'VARCHAR(255)'})

        # 在不同板块中循环
        for api in self.TUSHARE_VIPAPI:
            # ----------------初始化状态----------------#
            self.TRACK_BOARD = api

            # ----------------执行查询----------------#
            time.sleep(1)  # 1s一次可以
            self.df_ts = self.TuShare.query(api_name=api, period=period, )

            # print(self.df_ts)
            if self.df_ts.empty:  # 判断为空
                continue

            # ---------------- 入库----------------#
            # 生成主键
            if self.TRACK_BOARD == 'fina_mainbz_vip':
                self.df_ts['ID'] = self.df_ts[['ts_code', 'end_date']].apply(
                    lambda x: hashlib.md5(str(x['ts_code'] + x['end_date']).encode('UTF-8')).hexdigest(), axis=1)
            else:
                self.df_ts['ID'] = self.df_ts[['ts_code', 'ann_date']].apply(
                    lambda x: hashlib.md5(str(x['ts_code'] + x['ann_date']).encode('UTF-8')).hexdigest(), axis=1)

            # 入库
            self.df_ts.fillna('', inplace=True)
            # self.df_ts=self.df_ts.where((self.df_ts.notna()),None)
            # self.df_ts=self.df_ts.where(self.df_ts.applymap(lambda x:True if str(x)!='nan' else False),None)
            # print(self.df_ts)

            self.SqlObj.insert_table(api, self.df_ts,
                                     {'INSERT_DATETIME': 'DATETIME NULL DEFAULT CURRENT_TIMESTAMP',
                                      'UPDATE_DATETIME': 'DATETIME NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP',
                                      'ID': 'VARCHAR(255)',
                                      'PK': 'ID', 'ann_date': 'DATE', })

            # ----------------输出状态----------------#
            print('[{}/{}] {} {}  '.format(self.SqlObj.cur.rowcount, self.df_ts.shape[0],
                                           self.TRACK_DATE, self.TRACK_BOARD, ))

        # 该日期完成
        save_date()

    # 获取历史信息
    def get_historical_data(self, date_list):

        # date_list = [str(i).replace(' 00:00:00', '') for i in
        #              (pd.date_range(start='20000331', end='20221231', freq='3M').format(date_format="%Y%m%d"))]
        for date in date_list:
            self.TRACK_DATE = date
            # 查询要爬取的date是否完成
            if self.TRACK_DATE in self.SqlObj.select_table('finished_date', ['date'])['date'].tolist():
                continue
            else:
                # print(self.SqlObj.select_table('finished_date', ['date'])['date'].tolist())
                self.get_financial_data(period=self.TRACK_DATE)

    # 实时监控爬虫程序
    def update_spider(self):
        date_new = [str(i).replace(' 00:00:00', '') for i in
                    (pd.date_range(start='20220331', end='20221231', freq='3M').format(date_format="%Y%m%d"))]
        # ----------------在不同日期中循环----------------#
        for date in date_new:
            # ----------------初始化状态----------------#
            self.TRACK_DATE = date
            # 在不同板块中循环
            # ----------------在不同版块中循环----------------#
            for api in self.TUSHARE_VIPAPI:
                # ----------------初始化状态----------------#
                self.TRACK_BOARD = api

                # ----------------执行查询----------------#
                time.sleep(1)  # 1s一次可以
                self.df_ts = self.TuShare.query(api_name=self.TRACK_BOARD, period=self.TRACK_DATE, )

                if not self.df_ts.empty:  # 判断为空
                    # 执行更新程序
                    # 生成主键
                    if self.TRACK_BOARD == 'fina_mainbz_vip':
                        self.df_ts['ID'] = self.df_ts[['ts_code', 'end_date']].apply(
                            lambda x: hashlib.md5(str(x['ts_code'] + x['end_date']).encode('UTF-8')).hexdigest(),
                            axis=1)
                    else:
                        self.df_ts['ID'] = self.df_ts[['ts_code', 'ann_date']].apply(
                            lambda x: hashlib.md5(str(x['ts_code'] + x['ann_date']).encode('UTF-8')).hexdigest(),
                            axis=1)

                    # 只保留前面100的行
                    self.df_ts = self.df_ts.loc[:100, :]
                    # 更新所有的新的表格
                    self.SqlObj.insert_table(table_name=self.TRACK_BOARD, df_values=self.df_ts)
                    self.SqlObj.update_table(table_name=self.TRACK_BOARD, df_values=self.df_ts)


# 用于多线程处理
class SpiderThead(threading.Thread):
    def __init__(self, threadID, name, delay, datelist):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.delay = delay
        self.datelist = datelist

    def run(self, ):
        print("开始线程：" + self.name)
        self.my_func()
        print("退出线程：" + self.name)

    # 自定义函数
    def my_func(self):
        app = TushareSpider()
        app.get_historical_data(self.datelist)
        app.SqlObj.close_cnx()


# 获取历史记录
def get_all_history():
    date_list = [str(i).replace(' 00:00:00', '') for i in
                 (pd.date_range(start='20000331', end='20221231', freq='3M').format(date_format="%Y%m%d"))]
    print(len(date_list))
    date_list_1 = date_list[80:90]
    date_list_2 = date_list[90:]
    date_list_3 = date_list[40:60]
    date_list_4 = date_list[60:80]

    # date_list_2 = date_list[20:30]
    # date_list_2 = date_list[30:40]
    # 创建新线程
    thread1 = SpiderThead(1, "Thread-1", 1, datelist=date_list_1)
    thread2 = SpiderThead(2, "Thread-2", 2, datelist=date_list_2)
    thread3 = SpiderThead(2, "Thread-3", 3, datelist=date_list_3)
    thread4 = SpiderThead(2, "Thread-4", 4, datelist=date_list_4)

    # 开启新线程
    thread1.start()
    thread2.start()
    thread3.start()
    thread4.start()
    thread1.join()
    thread2.join()
    thread3.join()
    thread4.join()
    print("退出主线程")


# 实时监控
def get_now():
    app = TushareSpider()
    app.update_spider()
    app.SqlObj.close_cnx()


if __name__ == '__main__':
    get_now()
