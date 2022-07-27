# 必要参数设置
MAX_PAGE = 165  # 最大页数
PAGE_TRACK = 1  # 追踪到了第几页
MAX_GET = 1  # 获取最大尝试次数
MAX_PARSE = 1  # 解析尝试最大次数
MAX_CSV = 1  # 文件保存最大次数
MAX_PROXY = 1  # 获取代理的最大次数
MAX_START = 1  # MAX_*的初始值
MAX_TRY = 4  # 最大尝试次数
FLAG = 0  # 用于标识，是否使用 url_omi() 函数

# 初始链接
URL_START = "http://q.10jqka.com.cn//index/index/board/all/field/zdf/order/desc/page/"
PARAMS = "/ajax/1/"

# 第一次爬取的 html 缺失的页面 的url 列表
# 先进先出的列表
PAGE_LIST = []

# 代理池接口
PROXY_POOL_API = "http://127.0.0.1:5555/random"

headers = [
    {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.5060.134 Safari/537.36 Edg/103.0.1264.71',
        'Cookie': 'Hm_lvt_60bad21af9c824a4a0530d5dbf4357ca=1658797633; Hm_lvt_78c58f01938e4d85eaf619eae71b4ed1=1658797633; Hm_lvt_f79b64788a4e377c608617fba4c736e2=1658797633; searchGuide=sg; Hm_lpvt_60bad21af9c824a4a0530d5dbf4357ca=1658818918; Hm_lpvt_78c58f01938e4d85eaf619eae71b4ed1=1658818918; Hm_lpvt_f79b64788a4e377c608617fba4c736e2=1658818918; v={v}'
    }
]

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
