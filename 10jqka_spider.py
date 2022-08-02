from jqka_spider import start_spider

if __name__ == '__main__':
    # print(show_tables())
    app = start_spider.FinancialSpider()
    while True:
        app.yjyg_update()
