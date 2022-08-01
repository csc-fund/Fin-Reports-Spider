import pandas as pd

# 数据库部分
MYSQL_HOST = 'rm-2ze1tizo5y2ws9k8fro.mysql.rds.aliyuncs.com'
MYSQL_NAME = 'root'
MYSQL_PASSWORD = 'Aa123456'
MYSQL_DATABASE = 'tushare'

# 必要参数设置
TUSHARE_VIPAPI = ['income_vip', 'balancesheet_vip', 'cashflow_vip', 'forecast_vip', 'express_vip', 'fina_mainbz_vip',
                  ]
# 爬取的日期
DATE_LIST = [str(i).replace(' 00:00:00', '') for i in
             pd.date_range(start='20000301', end='20221231', freq='3M').tolist()]

# 初始链接

# 要爬取的栏目
