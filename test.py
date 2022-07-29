#
# This small example shows you how to access JS-based requests via Selenium
# Like this, one can access raw data for scraping,
# for example on many JS-intensive/React-based websites
#

from time import sleep

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support import expected_conditions as EC

from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager
import selenium

from selenium.webdriver.common.by import By
from selenium import webdriver
from selenium.webdriver import DesiredCapabilities
import json

import PyChromeDevTools

# make chrome log requests
# capabilities = DesiredCapabilities.CHROME
# # capabilities["loggingPrefs"] = {"performance": "ALL"}  # newer: goog:loggingPrefs
# driver = webdriver.Chrome(
#     desired_capabilities=capabilities, service=Service(ChromeDriverManager().install())
# )

# chrome = PyChromeDevTools.ChromeInterface()
# # fetch a site that does xhr requests
# driver.get(
#     "http://data.10jqka.com.cn/ajax/yjgg/date/2022-06-30/board/ALL/field/enddate/order/desc/page/1/ajax/1/free/1/")
# sleep(5)  # wait for the requests to take place
# print(driver.execute_cdp_cmd("Network.getCookies", {}))
import PyChromeDevTools
import time
import json
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support import expected_conditions as EC

from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager
import selenium

from selenium.webdriver.common.by import By
import fake_useragent
# options = webdriver.ChromeOptions()
# # options.add_argument('--headless')
# options.add_argument(
#     'User-Agent="{0}"'.format(fake_useragent.UserAgent().random))
# # options.add_argument('--disable-blink-features=AutomationControlled')
# # options.add_argument("--disable-extensions")
# # options.add_experimental_option('useAutomationExtension', False)
# # options.add_experimental_option("excludeSwitches", ["enable-automation"])
# options.add_experimental_option("debuggerAddress", "127.0.0.1:9222")
#
# # -------------驱动操作-------------#
# # driver.maximize_window()
# # driver.minimize_window()  # 将浏览器最大化显示
# driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
# driver.get(url='http://data.10jqka.com.cn/ajax/yjgg/date/2022-06-30/board/ALL/field/enddate/order/desc/page/1/ajax/1/free/1/')


chrome = PyChromeDevTools.ChromeInterface()
chrome.Network.enable()
chrome.Page.enable()
# chrome = PyChromeDevTools.ChromeInterface()
chrome.Page.navigate(
    url="http://data.10jqka.com.cn/ajax/yjgg/date/2022-06-30/board/ALL/field/enddate/order/desc/page/1/ajax/1/free/1/")
chrome.wait_event("Page.frameStoppedLoading", timeout=60)

# Wait last objects to load
time.sleep(5)

cookies, messages = chrome.Network.getCookies()
for cookie in cookies["result"]["cookies"]:
    # print("Cookie:")
    # print("\tDomain:", cookie["domain"])
    # print("\tKey:", cookie["name"])
    print("\tValue:", cookie["value"])
    # print("\n")

