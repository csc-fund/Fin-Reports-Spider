import pandas as pd

# 必要参数设置
MAX_PAGE = 0  # 最大页数
PAGE_TRACK = 1  # 追踪到了第几页
MAX_GET = 1  # 获取最大尝试次数
MAX_PARSE = 1  # 解析尝试最大次数
MAX_CSV = 1  # 文件保存最大次数
MAX_PROXY = 1  # 获取代理的最大次数
MAX_START = 1  # MAX_*的初始值
MAX_COOKIE = 4  # COOKIE最大尝试次数
MAX_TRY = 5  # 最大尝试次数
FLAG = 0  # 用于标识，是否使用 url_omi() 函数

STATU_DICT = {0: '获取页码长度', 1: '获取页面内容', 2: '解析数据入库', 3: '重复URL'}
# ERROR_DICT={0:'页码页面'}
# 初始链接
BASE_URl = "http://data.10jqka.com.cn/ajax/{board}/date/{date}/board/ALL/field/enddate/order/desc/page/{page}/ajax/1/free/1/"

# 要爬取的栏目
# 栏目yypl异常,yjkb位置已经调整
# BOARD_LIST = ['yjkb', 'yjyg', 'yjgg', 'sgpx', 'ggjy']
# BOARD_LIST = ['yjgg', 'yjkb', 'yjyg', ]
BOARD_LIST = ['yjyg' ]
DATE_LIST = [str(i).replace(' 00:00:00', '') for i in
             pd.date_range(start='20220301', end='20221231', freq='3M').tolist()]
# 第一次爬取的 html 缺失的页面 的url 列表
# 先进先出的列表
PAGE_LIST = []

# 代理池接口
PROXY_POOL_API = "http://127.0.0.1:5555/random"

# COOKIES
BASE_COOKIES = 'Hm_lvt_60bad21af9c824a4a0530d5dbf4357ca=1658797633; Hm_lvt_78c58f01938e4d85eaf619eae71b4ed1=1658797633; Hm_lvt_f79b64788a4e377c608617fba4c736e2=1658797633; searchGuide=sg; Hm_lpvt_60bad21af9c824a4a0530d5dbf4357ca=1658818918; Hm_lpvt_78c58f01938e4d85eaf619eae71b4ed1=1658818918; Hm_lpvt_f79b64788a4e377c608617fba4c736e2=1658818918; v={v}'

# 数据库部分
MONGO_URL = ''
db_struct = [{'ID': 'VARCHAR(100)',
              '股票代码': 'int', '股票简称': 'VARCHAR(10)', '业绩预告类型': 'VARCHAR(100)',
              '业绩预告摘要': 'VARCHAR(100)',
              '上年同期净利润': 'VARCHAR(10)', '公告日期': 'DATE', 'PK': 'ID'
              },
             {'ID': 'VARCHAR(100)',
              '股票代码': 'int', '股票简称': 'VARCHAR(10)', '业绩预告类型': 'VARCHAR(100)',
              '业绩预告摘要': 'VARCHAR(100)',
              '上年同期净利润': 'VARCHAR(10)', '公告日期': 'DATE', 'PK': 'ID'
              },
             {'ID': 'VARCHAR(100)',
              '股票代码': 'int', '股票简称': 'VARCHAR(10)', '业绩预告类型': 'VARCHAR(100)',
              '业绩预告摘要': 'VARCHAR(100)',
              '上年同期净利润': 'VARCHAR(10)', '公告日期': 'DATE', 'PK': 'ID'
              },
             {'ID': 'VARCHAR(100)',
              '股票代码': 'int', '股票简称': 'VARCHAR(10)', '业绩预告类型': 'VARCHAR(100)',
              '业绩预告摘要': 'VARCHAR(100)',
              '上年同期净利润': 'VARCHAR(10)', '公告日期': 'DATE', 'PK': 'ID'
              },
             {'ID': 'VARCHAR(100)',
              '股票代码': 'int', '股票简称': 'VARCHAR(10)', '业绩预告类型': 'VARCHAR(100)',
              '业绩预告摘要': 'VARCHAR(100)',
              '上年同期净利润': 'VARCHAR(10)', '公告日期': 'DATE', 'PK': 'ID'
              },
             {'ID': 'VARCHAR(100)',
              '股票代码': 'int', '股票简称': 'VARCHAR(10)', '业绩预告类型': 'VARCHAR(100)',
              '业绩预告摘要': 'VARCHAR(100)',
              '上年同期净利润': 'VARCHAR(10)', '公告日期': 'DATE', 'PK': 'ID'
              },
             ]
