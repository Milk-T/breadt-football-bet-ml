# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

from .items import FSpiderBriefInfo, FSpiderReferInfo, FSpiderPredictInfo, FSpiderFeatureInfo
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
                sql = "INSERT INTO `breadt_football_game_list` (`matchid`, `status`, `game`, `turn`, `home_team`, `visit_team`, `gs`, `gd`, `gn`, `time`, `result`, `win_bet_return`, `draw_bet_return`, `lose_bet_return`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
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

        elif isinstance(item, FSpiderFeatureInfo):
            with self.connection.cursor() as cursor:
                sql = """
                INSERT INTO `breadt_football_feature_info` 
                (`matchid`, 

                `h_score`, 
                `h_rank`,

                `h_perf_win`, `h_perf_draw`, `h_perf_lose`, 
                `h_host_win`, `h_host_draw`, `h_host_lose`, 
                `h_battle_with_front_10_win`, `h_battle_with_front_10_draw`, `h_battle_with_front_10_lose`, 
                `h_battle_with_end_10_win`, `h_battle_with_end_10_draw`, `h_battle_with_end_10_lose`, 

                `h_perf_gs`, `h_perf_gd`, `h_perf_avg_gs`, `h_perf_avg_gd`,
                `h_host_gs`, `h_host_gd`, `h_host_avg_gs`, `h_host_avg_gd`,
                `h_r3_gs`, `h_r3_gd`, `h_r3_avg_gs`, `h_r3_avg_gd`,

                `h_perf_bet_high`, `h_perf_bet_low`, `h_host_bet_high`, `h_host_bet_low`,
                `h_host_0_1_goal`, `h_host_2_3_goal`, `h_host_ab_4_goal`, 
                `h_host_0_goal`, `h_host_1_goal`, `h_host_2_goal`, `h_host_3_goal`, `h_host_4_goal`, `h_host_5_goal`, `h_host_6_goal`, `h_host_7_goal`,

                `v_score`, 
                `v_rank`, 

                `v_perf_win`, `v_perf_draw`, `v_perf_lose`, 
                `v_host_win`, `v_host_draw`, `v_host_lose`, 
                `v_battle_with_front_10_win`, `v_battle_with_front_10_draw`, `v_battle_with_front_10_lose`, 
                `v_battle_with_end_10_win`, `v_battle_with_end_10_draw`, `v_battle_with_end_10_lose`, 

                `v_perf_gs`, `v_perf_gd`, `v_perf_avg_gs`, `v_perf_avg_gd`,
                `v_host_gs`, `v_host_gd`, `v_host_avg_gs`, `v_host_avg_gd`,
                `v_r3_gs`, `v_r3_gd`, `v_r3_avg_gs`, `v_r3_avg_gd`,

                `v_perf_bet_high`, `v_perf_bet_low`, `v_host_bet_high`, `v_host_bet_low`,
                `v_host_0_1_goal`, `v_host_2_3_goal`, `v_host_ab_4_goal`, 
                `v_host_0_goal`, `v_host_1_goal`, `v_host_2_goal`, `v_host_3_goal`, `v_host_4_goal`, `v_host_5_goal`, `v_host_6_goal`, `v_host_7_goal`


                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s
                )
                """
                cursor.execute(sql, (
                    item['matchid'],
                    item['h_score'], item['h_rank'],
                    item['h_perf_win'], item['h_perf_draw'], item['h_perf_lose'],
                    item['h_host_win'], item['h_host_draw'], item['h_host_lose'],
                    item['h_battle_with_front_10_win'], item['h_battle_with_front_10_draw'], item['h_battle_with_front_10_lose'],
                    item['h_battle_with_end_10_win'], item['h_battle_with_end_10_draw'], item['h_battle_with_end_10_lose'],

                    item['h_perf_gs'], item['h_perf_gd'], item['h_perf_avg_gs'], item['h_perf_avg_gd'],
                    item['h_host_gs'], item['h_host_gd'], item['h_host_avg_gs'], item['h_host_avg_gd'],
                    item['h_r3_gs'], item['h_r3_gd'], item['h_r3_avg_gs'], item['h_r3_avg_gd'],

                    item['h_perf_bet_high'], item['h_perf_bet_low'], item['h_host_bet_high'], item['h_host_bet_low'],
                    item['h_host_0_1_goal'], item['h_host_2_3_goal'], item['h_host_ab_4_goal'],
                    item['h_host_0_goal'], item['h_host_1_goal'], item['h_host_2_goal'], item['h_host_3_goal'], item[
                        'h_host_4_goal'], item['h_host_5_goal'], item['h_host_6_goal'], item['h_host_7_goal'],

                    item['v_score'], item['v_rank'],
                    item['v_perf_win'], item['v_perf_draw'], item['v_perf_lose'],
                    item['v_host_win'], item['v_host_draw'], item['v_host_lose'],
                    item['v_battle_with_front_10_win'], item['v_battle_with_front_10_draw'], item['v_battle_with_front_10_lose'],
                    item['v_battle_with_end_10_win'], item['v_battle_with_end_10_draw'], item['v_battle_with_end_10_lose'],

                    item['v_perf_gs'], item['v_perf_gd'], item['v_perf_avg_gs'], item['v_perf_avg_gd'],
                    item['v_host_gs'], item['v_host_gd'], item['v_host_avg_gs'], item['v_host_avg_gd'],
                    item['v_r3_gs'], item['v_r3_gd'], item['v_r3_avg_gs'], item['v_r3_avg_gd'],

                    item['v_perf_bet_high'], item['v_perf_bet_low'], item['v_host_bet_high'], item['v_host_bet_low'],
                    item['v_host_0_1_goal'], item['v_host_2_3_goal'], item['v_host_ab_4_goal'],
                    item['v_host_0_goal'], item['v_host_1_goal'], item['v_host_2_goal'], item['v_host_3_goal'],
                    item['v_host_4_goal'], item['v_host_5_goal'], item['v_host_6_goal'], item['v_host_7_goal'],
                )
                )

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
