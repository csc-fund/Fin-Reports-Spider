import requests
from urllib.parse import quote
from bs4 import BeautifulSoup
import fake_useragent
import sys
import time
import random
import PyChromeDevTools
from fake_useragent import UserAgent
import csv
import pandas as pd
from mysql_dao import *

import hashlib

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support import expected_conditions as EC

from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager
import selenium

from selenium.webdriver.common.by import By

from settings import *


class FinancialSpider:
    # 从配置文件初始化参数
    def __init__(self):
        # 设置
        self.BASE_URl = BASE_URl  # 基本URL
        self.BASE_COOKIES = BASE_COOKIES  # COOKIES
        self.BOARD_LIST = BOARD_LIST  # 要爬取的栏目
        self.DATE_LIST = DATE_LIST  # 要爬取的日期
        self.MAX_PAGE = MAX_PAGE  # 最大的页数
        self.MAX_COOKIE = MAX_COOKIE  # 最大COOKIE尝试次数
        self.STATU_DICT = STATU_DICT  # 爬虫状态列表

        # 当前状态信息
        self.BOARD_TRACK = ''  # 跟踪栏目
        self.DATE_TRACK = ''  # 跟踪日期
        self.PAGE_TRACK = ''  # 跟踪页数
        self.URl_TRACK = ''  # 跟踪UR
        self.CRAWL_STATU = None  # 当前爬虫的状态
        self.UA_TRACK = None  # 当前的UA
        self.COOKIES_TRACK = None  # 当前的COOKIES
        self.REQUEST_TRACK = None  # 跟踪request

        # 共用的Chrome对象
        self.CHROME = PyChromeDevTools.ChromeInterface()
        self.CHROME.Network.enable()
        self.CHROME.Page.enable()

    # 按照栏目爬取
    def board_crawl(self):
        # 存储已经爬取过的url
        def save_url():
            df_url = pd.DataFrame(data=[self.URl_TRACK], columns=['url'])
            insert_table('finished_url', df_url, {'PK': 'url'})

        # 生成cookies验证
        def get_cookies():
            for i in range(MAX_COOKIE):
                # -------------驱动设置-------------#
                options = webdriver.ChromeOptions()
                # options.add_argument('--headless')
                options.add_argument(
                    'User-Agent="{0}"'.format(self.UA_TRACK))
                options.add_argument('--disable-blink-features=AutomationControlled')
                options.add_argument("--disable-extensions")
                options.add_experimental_option('useAutomationExtension', False)
                options.add_experimental_option("excludeSwitches", ["enable-automation"])

                # -------------驱动操作-------------#
                # driver.maximize_window()
                # driver.minimize_window()  # 将浏览器最大化显示
                driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
                driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
                # driver.set_page_load_timeout(8)
                driver.get(url=self.URl_TRACK)

                # driver.implicitly_wait(5)
                # 设置显式等待，超时时长最大为5s，每隔0.5s查找元素一次
                element = WebDriverWait(driver, 10, 1).until(
                    EC.presence_of_element_located((By.CLASS_NAME, 'J-ajax-table')))
                print(element)
                # time.sleep(2)
                v = driver.get_cookie('v')
                print(v)
                # time.sleep(1111)
                driver.delete_all_cookies()
                driver.quit()
                if v:
                    print(v['value'])
                    return v['value']
                else:
                    time.sleep(random.random() * 3)  # 设置延时
                    print('重新获取')
                    continue

            # time.sleep(2)
            # # 获取cookie中的name和value,转化成requests可以使用的形式
            # cookies = {}
            # for cookie in c:
            #     cookies[cookie['name']] = cookie['value']
            # time.sleep(1)

            # if cookies != {}:
            #     break
            # else:
            #     print(c)
            #     time.sleep(random.random() * 3)  # 设置延时
            #     continue

        # 新方法生成cookies验证
        def get_cookies_V2():
            while True:
                # -------------访问URL-------------#
                self.CHROME.Page.navigate(url=self.URl_TRACK)
                self.CHROME.wait_event("Page.frameStoppedLoading", timeout=10)
                time.sleep(2)

                # -------------cookies-------------#
                cookies, messages = self.CHROME.Network.getCookies()
                for cookie in cookies["result"]["cookies"]:
                    # print("Value:", cookie["value"])
                    self.COOKIES_TRACK = cookie["value"]
                    return cookie["value"]

        # 获取最大页面
        def get_maxpage():
            # -------------- 初始化状态参数 -------------- #
            self.MAX_PAGE = 0  # 初始化当前日期的最大页数
            self.CRAWL_STATU = self.STATU_DICT[0]
            self.PAGE_TRACK = 0
            self.REQUEST_TRACK = None
            self.URl_TRACK = self.BASE_URl.format(board=self.BOARD_TRACK,
                                                  date=self.DATE_TRACK,
                                                  page=1)

            # 随机生成UA
            self.UA_TRACK = fake_useragent.UserAgent().random

            # 使用获得的cookies爬取
            self.REQUEST_TRACK = requests.get(url=self.URl_TRACK,
                                              headers={'User-Agent': self.UA_TRACK,
                                                       'Cookie': self.BASE_COOKIES.format(v=get_cookies_V2())},
                                              timeout=5)

            soup = BeautifulSoup(self.REQUEST_TRACK.content.decode('gbk'), 'lxml')

            # 判断当前页面数据是否存在
            data_selector = soup.select('.tc')
            if data_selector and data_selector[0].text == '今日无数据':
                self.MAX_PAGE = 0

            #  存在则找页码page_info
            else:
                self.MAX_PAGE = 1
                page_selector = soup.select('.page_info')
                if page_selector:
                    self.MAX_PAGE = int(page_selector[0].text.split('/')[1])

            # 如果最大页码是0就抛出异常
            assert self.MAX_PAGE >= 1

        # 获取page中的request
        def get_page():
            # -------------- 初始化状态参数 -------------- #
            self.CRAWL_STATU = self.STATU_DICT[1]
            self.URl_TRACK = self.BASE_URl.format(board=self.BOARD_TRACK,
                                                  date=self.DATE_TRACK,
                                                  page=self.PAGE_TRACK)
            self.REQUEST_TRACK = None

            # 查询要爬取的url是否完成
            if self.URl_TRACK in select_table('finished_url', ['url'])['url'].tolist():
                self.CRAWL_STATU = self.STATU_DICT[3]
                return

            # 降低速度
            time.sleep(random.random() * 5)  # 设置延时

            # 随机生成UA
            self.UA_TRACK = fake_useragent.UserAgent().random

            # 使用获得的cookies爬取
            self.REQUEST_TRACK = requests.get(url=self.URl_TRACK,
                                              headers={'User-Agent': self.UA_TRACK,
                                                       'Cookie': self.BASE_COOKIES.format(v=get_cookies_V2())},
                                              timeout=5)
            #     解析返回的requests
            get_content()

        # 解析模块
        def get_content():
            # -------------- 初始化状态参数 -------------- #

            soup = BeautifulSoup(self.REQUEST_TRACK.content.decode('gbk'), 'lxml')

            table = soup.select('.J-ajax-table')[0]
            # print(table)

            # 表头
            record_th = []
            for tr in table.select('thead tr'):
                fields = tr.select('th')
                record_th += [i.text for i in fields if i.attrs.get('class') != ['th-col']]

            # 去掉序号
            record_th = record_th[1:]

            # yjkb有2行,特殊处理
            if self.BOARD_TRACK == 'yjkb':
                record_th = [v + str(record_th[:i].count(v) + 1) if record_th.count(v) > 1 else v for i, v in
                             enumerate(record_th)]
                # 重新排序
                record_th = record_th[0:3] + record_th[7:15] + record_th[3:7]

            # yjgg,特殊处理
            elif self.BOARD_TRACK == 'yjgg':
                record_th = [v + str(record_th[:i].count(v) + 1) if record_th.count(v) > 1 else v for i, v in
                             enumerate(record_th)]
                # 重新排序
                record_th = record_th[0:3] + record_th[9:17] + record_th[3:9]



            # 表格内容
            df_list = pd.DataFrame()
            for tr in table.select('tbody tr'):
                fields = tr.select('td')
                # 将每行记录第一列去掉，第一列为序号，没有存储必要
                record = [field.text.strip() for field in fields[1:]]
                # 拼接入库
                df_list = pd.concat([df_list, pd.DataFrame(record).transpose()], axis=0, ignore_index=True)

            # 表头和内容要匹配
            assert len(record_th) == df_list.shape[1]

            # 重命名表头
            df_list.columns = record_th

            # 用前4列的字符串生成唯一的md5标识作为pk
            df_list['ID'] = df_list.iloc[:, 0:4].apply(
                lambda x: hashlib.md5(str(x[0] + x[1] + x[2] + x[3]).encode('UTF-8')).hexdigest(), axis=1)


            # 入库存储
            if not df_list.empty:
                insert_table(self.BOARD_TRACK, df_list,
                             {'ID': 'VARCHAR(255)', 'PK': 'ID'})
                # 更新url为已经爬取
                save_url()
                self.CRAWL_STATU = self.STATU_DICT[2]

            else:
                print('空')



        # 在栏目中循环
        def start_crawl():
            # --------------在栏目中循环--------------#
            for board in self.BOARD_LIST:
                self.BOARD_TRACK = board  # 当前爬取的栏目

                # --------------在日期中循环--------------#
                for date in self.DATE_LIST:
                    self.DATE_TRACK = date  # 当前爬取的日期
                    try:
                        # --------------获取最大页码--------------#
                        get_maxpage()
                    except AssertionError as e:  # 更新页码后没变
                        print('更新后页面为:{}'.format(self.MAX_PAGE))
                        continue

                    # --------------在页面中中循环--------------#
                    for page in range(self.MAX_PAGE):
                        self.PAGE_TRACK = page + 1  # 当前爬取的页面
                        statu_str = "[ {} ] {} {} {} {} {} {}".format(self.CRAWL_STATU, self.BOARD_TRACK,
                                                                      self.DATE_TRACK,
                                                                      self.PAGE_TRACK, self.MAX_PAGE,
                                                                      self.URl_TRACK,
                                                                      self.COOKIES_TRACK)
                        try:
                            # --------------获取每个页面--------------#
                            get_page()
                            print(statu_str)
                        except IndexError as e:
                            print('IndexError失败,推测为网络原因\n{}'.format(statu_str))
                            continue
                        except ConnectionError as e:
                            print('ConnectionError推测为网络原因\n{}'.format(statu_str))
                            continue
                        except requests.exceptions.ReadTimeout as e:
                            print('HTTPConnectionPool\n{}'.format(statu_str))
                            continue

        # 循环爬取
        start_crawl()


if __name__ == '__main__':
    # print(show_tables())
    app = FinancialSpider()
    app.board_crawl()
