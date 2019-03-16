# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

from .items import FSpiderBriefInfo, FSpiderReferInfo
import pandas as pd
import os
import random


class FootballGameInfoPipeline(object):

    def open_spider(self, spider):
        print('FootballGameInfoPipeline.open_spider')

        # self.df = pd.DataFrame()
        # self.detail = pd.DataFrame()

        if os.path.exists('../data/f.brief.pkl'):
            self.df = pd.read_pickle('../data/f.brief.pkl')
        else:
            self.df = pd.DataFrame()

        if os.path.exists('../data/f.refer.pkl'):
            self.detail = pd.read_pickle('../data/f.refer.pkl')
        else:
            self.detail = pd.DataFrame()
        
    def process_item(self, item, spider):
        print('FootballGameInfoPipeline.process_item...')

        if type(item) is FSpiderBriefInfo:
            self.df = self.df.append(item.__dict__['_values'], ignore_index=True)
        elif type(item) is FSpiderReferInfo:
            self.detail = self.detail.append(item.__dict__['_values'], ignore_index=True)

        random_num = random.randint(0,300)
        if random_num % 100 == 0:
            if len(self.detail) > 0:
                self.detail.to_pickle('../data/f.refer.pkl')

        return item

    def close_spider(self, spider):
        if len(self.df) > 0:
            self.df.to_pickle('../data/f.brief.pkl')

        if len(self.detail) > 0:
            self.detail.to_pickle('../data/f.refer.pkl')

        