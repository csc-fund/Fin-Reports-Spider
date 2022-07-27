class Log:
    logging, logger = None, None
    formatter, cons_handler, file_handler = None, None, None

    def __init__(self):
        import logging
        self.logging = logging
        # 创建logger实例
        self.logger = logging.getLogger('logtop')
        # 设置logger实例的等级
        self.logger.setLevel(logging.INFO)
        # 创建formatter
        self.formatter = logging.Formatter('%(asctime)s - %(pathname)s[line:%(lineno)d] - %(levelname)s: %(message)s')

    def set_stream_level(self, str):
        # 创建控制台handler
        self.cons_handler = self.logging.StreamHandler()
        self.cons_handler.setLevel(self.logging.WARN)
        self.cons_handler.setFormatter(self.formatter)

        # 创建文件handler
        self.file_handler = self.logging.FileHandler('testlog.log', mode='w')
        self.file_handler.setLevel(self.logging.INFO)
        self.file_handler.setFormatter(self.formatter)

        # 添加handler到logger
        self.logger.addHandler(self.cons_handler)
        self.logger.addHandler(self.file_handler)

        self.logger.info(str)


ml = Log()
