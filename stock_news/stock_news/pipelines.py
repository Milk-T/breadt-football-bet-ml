# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html

import pandas as pd
from .items import SSpiderBriefInfo, SSpiderInfoBriefInfo
import pymysql.cursors



class StockNewsPipeline(object):

    def connect(self):
        self.connection = pymysql.connect(host='localhost',
                                     user='root',
                                     password='breadt@2019',
                                     db='stock_info',
                                     charset='utf8mb4',
                                     cursorclass=pymysql.cursors.DictCursor)

    def open_spider(self, spider):
        self.df = pd.DataFrame()
        # self.info = pd.DataFrame()

        self.connect()


    def process_item(self, item, spider):
        if isinstance(item, SSpiderBriefInfo):
            self.df = self.df.append(item.__dict__['_values'], ignore_index=True)
        elif isinstance(item, SSpiderInfoBriefInfo):
            # self.info = self.info.append(item.__dict__['_values'], ignore_index=True)
            with self.connection.cursor() as cursor:
                sql = "INSERT INTO `stock_info` (`code`, `title`, `link`, `date`) VALUES (%s, %s, %s, %s)"
                cursor.execute(sql, (item['code'], item['title'], item['link'], item['date']))

            self.connection.commit()

        return item

    def close_spider(self, spider):
        if len(self.df) > 0:
            self.df.to_pickle('../data/s.list.pkl')

        # if len(self.info) > 0:
        #     self.info.to_pickle('../data/s.info.pkl')

        self.connection.close()
