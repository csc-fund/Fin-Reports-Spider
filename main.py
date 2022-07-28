import requests
from urllib.parse import quote
from settings import *
from bs4 import BeautifulSoup
import fake_useragent
import sys
import time
import random
from fake_useragent import UserAgent
import csv
import pandas as pd
from mysql_dao import *
import settings
import hashlib
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import selenium


class FinancialAna:
    def __init__(self):
        self.BASE_URl = 'http://data.10jqka.com.cn/ajax/{board}/date/{date}/board/ALL/field/enddate/order/desc/page/{page}/ajax/1/free/1/'
        # 请求头
        self.headers_list = headers
        # 网页参数
        self.MAX_PAGE = 0

        # 栏目yypl异常,yjkb位置已经调整
        self.board = ['yjkb', 'yjyg', 'yjgg', 'sgpx', 'ggjy']
        # self.board = ['yjkb']

    # 按照栏目爬取
    def board_crawl(self):

        # 生成时间序列
        def get_date_list():
            date_list = pd.date_range(start='20040301', end='20220301', freq='3M').tolist()
            date_list = [str(i).replace(' 00:00:00', '') for i in date_list]
            return date_list

        # 生成cookies验证
        def get_cookies(url, ua):

            options = webdriver.ChromeOptions()
            # options.add_argument('--headless')
            options.add_argument(
                'User-Agent="{0}"'.format(ua))
            options.add_argument('--disable-blink-features=AutomationControlled')
            options.add_argument("--disable-extensions")
            options.add_experimental_option('useAutomationExtension', False)
            options.add_experimental_option("excludeSwitches", ["enable-automation"])

            # 设置中文
            # 新版驱动设置
            driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
            # driver = webdriver.Chrome(executable_path='/Users/mac/Downloads/chromedriver', options=options)
            driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            # driver.maximize_window()
            driver.get(url=url)
            driver.implicitly_wait(2)
            c = driver.get_cookies()
            time.sleep(2)
            # # 获取cookie中的name和value,转化成requests可以使用的形式
            cookies = {}
            for cookie in c:
                cookies[cookie['name']] = cookie['value']
            time.sleep(1)
            driver.quit()
            # print(cookies)
            return cookies['v']

        # 获取request
        def get_request(url):

            # 查询要爬取的url是否完成
            if url in select_table('finished_url', ['url'])['url'].tolist() and self.MAX_PAGE != 0:

                return None
            # 降低速度
            time.sleep(random.random() * 5)  # 设置延时

            # 请求头
            ua = fake_useragent.UserAgent().random
            try:
                header = {'User-Agent': ua, 'Cookie': headers[0]['Cookie'].format(v=get_cookies(url, ua))}
            # 网络问题
            except KeyError as e:

                print(e)
                return None

            # 使用获得的cookies爬取
            web_source = requests.get(url=url, headers=header, timeout=5)

            if web_source.status_code == 200 and web_source is not None:
                # 解析最大的页数
                if self.MAX_PAGE == 0:
                    soup = BeautifulSoup(web_source.content.decode('gbk'), 'lxml')
                    try:
                        self.MAX_PAGE = int(soup.select('.page_info')[0].text.split('/')[1])
                        print('获取到页面总数:', url, soup.select('.page_info'))
                    except IndexError:
                        return None
                        # self.MAX_PAGE = 0
                # 更新过页面
                else:
                    return web_source
            else:
                # print(web_source.status_code)
                return None

        # 在栏目中循环
        def get_board():
            for board in self.board:


                # 在日期中循环
                for date in get_date_list():
                    # 初始化栏目的最大页数
                    self.MAX_PAGE = 0

                    # 解析第1页更新页码
                    try:
                        get_request(self.BASE_URl.format(date=date, board=board, page=1))
                    except Exception as e:
                        print(e)
                        continue

                    # 更新页码后解析剩余的页
                    for i in range(self.MAX_PAGE):

                        # 待爬取url
                        url = self.BASE_URl.format(date=date, board=board, page=(i + 1))
                        print('正在爬取', url, board, date, i + 1)
                        try:
                            re = get_request(url)
                        except Exception as e:
                            print(e)

                            continue

                        # 如果不是状态错误,已经完成,就开始解析
                        if re:
                            try:
                                self.items_return(board, re, url)
                                # print(url)
                            except AssertionError as e:
                                print(e)
                                continue

                        else:
                            # get出错的时候跳过
                            continue

        # 循环爬取栏目
        get_board()

    # 解析模块
    def items_return(self, board, web_source, this_url):
        print('解析模块')

        # 存储已经爬取过的模块
        def save_done_url(url):
            df_url = pd.DataFrame(data=[url], columns=['url'])
            insert_table('finished_url', df_url, {'PK': 'url'})
            print('已入库')

        def parse_web():
            soup = BeautifulSoup(web_source.content.decode('gbk'), 'lxml')

            table = soup.select('.J-ajax-table')[0]

            # 表头
            record_th = []
            for tr in table.select('thead tr'):
                fields = tr.select('th')
                record_th += [i.text for i in fields if i.attrs.get('class') != ['th-col']]

            # 去掉序号
            record_th = record_th[1:]

            # yjkb有2行,特殊处理
            if board == 'yjkb':
                record_th = [v + str(record_th[:i].count(v) + 1) if record_th.count(v) > 1 else v for i, v in
                             enumerate(record_th)]
                # 重新排序
                record_th = record_th[0:3] + record_th[7:15] + record_th[3:7]
            # print(record_th)
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
            print('入库存储')
            try:
                insert_table(board, df_list,
                             {'ID': 'VARCHAR(255)', 'PK': 'ID'})
                # 更新url为已经爬取
                save_done_url(this_url)
            except TypeError as e :
                print(df_list,e)




        # 解析html
        parse_web()


if __name__ == '__main__':
    # print(show_tables())
    app = FinancialAna()
    app.board_crawl()
