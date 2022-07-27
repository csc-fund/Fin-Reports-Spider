#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @FileName  :bar.py
# @Time      :2022/6/26 18:51
# @Author    :Colin
# @Note      :None


from tqdm import tqdm


class Bar:
    def __init__(self, des: str, total: int):
        self.tq = tqdm(range(total), unit='img')
        self.tq.set_description(des)

    def get_bar(self):
        return self.tq

    def finish(self):
        self.tq.update(self.tq.total - self.tq.n)

#
# b = Bar('', 100).get_bat()
# b.update(11)
# b.update(b.total - b.n)
# # print(b.total)
# # print(b.n, b.total)
