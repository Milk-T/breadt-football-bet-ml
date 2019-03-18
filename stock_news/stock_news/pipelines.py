# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html

import pandas as pd
from .items import SSpiderBriefInfo


class StockNewsPipeline(object):

    def open_spider(self, spider):
        self.df = pd.DataFrame()

    def process_item(self, item, spider):
        if isinstance(item, SSpiderBriefInfo):
            self.df = self.df.append(item.__dict__['_values'], ignore_index=True)

        return item

    def close_spider(self, spider):
        self.df.to_pickle('../data/s.list.pkl')
