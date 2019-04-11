# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

from .items import FSpiderBriefInfo, FSpiderReferInfo, FSpiderPredictInfo
import pandas as pd
import os
import random
import pymysql.cursors


class FootballGameInfoPipeline(object):

    def connect(self):
        self.connection = pymysql.connect(host='localhost',
                                          user='root',
                                          password='breadt@2019',
                                          db='breadt-football-ml',
                                          charset='utf8mb4',
                                          cursorclass=pymysql.cursors.DictCursor)

    def open_spider(self, spider):
        print('FootballGameInfoPipeline.open_spider')

        # self.df = pd.DataFrame()
        # self.detail = pd.DataFrame()

        # if os.path.exists('../data/f.brief.pkl'):
        #     self.df = pd.read_pickle('../data/f.brief.pkl')
        # else:
        #     self.df = pd.DataFrame()

        # if os.path.exists('../data/f.refer.pkl'):
        #     self.detail = pd.read_pickle('../data/f.refer.pkl')
        # else:
        #     self.detail = pd.DataFrame()

        self.connect()

    def process_item(self, item, spider):
        print('FootballGameInfoPipeline.process_item...')

        if isinstance(item, FSpiderBriefInfo):
            with self.connection.cursor() as cursor:
                sql = "INSERT INTO `breadt_football_game_list` (`matchid`, `status`, `game`, `turn`, `home_team`, `visit_team`, `gs`, `gd`, `gn`, `time`, `result`, `win_bet_return`, `draw_bet_return`, `lose_bet_return`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s)"
                cursor.execute(sql, (item['matchid'], item['status'], item['game'], item['turn'], item['home_team'], item['visit_team'], item['gs'],
                                     item['gd'], item['gn'], item['time'], item['result'], item['win_bet_return'], item['draw_bet_return'], item['lose_bet_return']))

            self.connection.commit()
        elif isinstance(item, FSpiderReferInfo):
            with self.connection.cursor() as cursor:
                sql = "INSERT INTO `breadt_football_refer_games` (`fid`, `pos`, `name`, `home_team`, `visit_team`, `gs`, `gd`, `gn`, `date`, `result`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                cursor.execute(sql, (item['fid'], item['pos'], item['name'], item['host_team'],
                                     item['visit_team'], item['gs'], item['gd'], item['gn'], item['date'], item['result']))

            self.connection.commit()
        elif isinstance(item, FSpiderPredictInfo):
            with self.connection.cursor() as cursor:
                sql = "INSERT INTO `breadt_football_predict_game` (`fid`, `status`, `game`, `turn`, `home_team`, `visit_team`, `offset`, `time`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
                cursor.execute(sql, (item['fid'], item['status'], item['game'], item['turn'],
                                     item['home_team'], item['visit_team'], item['offset'], item['time']))

            self.connection.commit()

        # if isinstance(item, FSpiderBriefInfo):
        #     self.df = self.df.append(
        #         item.__dict__['_values'], ignore_index=True)
        # elif isinstance(item, FSpiderReferInfo):
        #     self.detail = self.detail.append(
        #         item.__dict__['_values'], ignore_index=True)

        # random_num = random.randint(0, 300)
        # if random_num % 100 == 0:
        #     if len(self.detail) > 0:
        #         self.detail.to_pickle('../data/f.refer.pkl')

        return item

    def close_spider(self, spider):
        self.connection.close()

        # if len(self.df) > 0:
        #     self.df.to_pickle('../data/f.brief.pkl')

        # if len(self.detail) > 0:
        #     self.detail.to_pickle('../data/f.refer.pkl')
