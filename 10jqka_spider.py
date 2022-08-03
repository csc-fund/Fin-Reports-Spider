from jqka_spider import start_spider

if __name__ == '__main__':
    # print(show_tables())
    app = start_spider.FinancialSpider()
    while True:
        for board in ['yjyg', 'yjgg', 'yjkb']:
            app.board_update(board)
