# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

from .items import FSpiderOffsetOddInfo, FSpiderBriefInfo, FSpiderReferInfo, FSpiderPredictInfo, FSpiderFeatureInfo, FSpiderLotteryInfo, FSpiderLotteryPredictInfo, FSpiderRecentFeatureInfo, FSpiderOddInfo
import pandas as pd
import os
import random
import pymysql.cursors


class FootballGameInfoPipeline(object):

    def connect(self):
        self.connection = pymysql.connect(host='10.12.86.109',
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

        elif isinstance(item, FSpiderLotteryInfo):
            with self.connection.cursor() as cursor:
                sql = "INSERT INTO `breadt_lottery_info` (`matchid`, `issue`, `status`, `game`, `turn`, `home_team`, `visit_team`, `gs`, `gd`, `gn`, `time`, `result`, `win_bet_return`, `draw_bet_return`, `lose_bet_return`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                cursor.execute(sql, (item['matchid'], item['issue'], item['status'], item['game'], item['turn'], item['home_team'], item['visit_team'], item['gs'],
                                     item['gd'], item['gn'], item['time'], item['result'], item['win_bet_return'], item['draw_bet_return'], item['lose_bet_return']))

            self.connection.commit()

        elif isinstance(item, FSpiderLotteryPredictInfo):
            with self.connection.cursor() as cursor:
                sql = "INSERT INTO `breadt_lottery_predict_info` (`matchid`, `issue`, `status`, `game`, `turn`, `home_team`, `visit_team`, `time`, `win_bet_return`, `draw_bet_return`, `lose_bet_return`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                cursor.execute(sql, (item['matchid'], item['issue'], item['status'], item['game'], item['turn'], item['home_team'], item['visit_team'],
                                     item['time'], item['win_bet_return'], item['draw_bet_return'], item['lose_bet_return']))

            self.connection.commit()

        elif isinstance(item, FSpiderRecentFeatureInfo):
            with self.connection.cursor() as cursor:
                sql = """
                INSERT INTO `breadt_football_recent_feature_info`
                (
                    `matchid`,
                    `h_abs_win`,
                    `h_abs_draw`,
                    `h_abs_lose`,
                    `h_count`,
                    `h_host_win`,
                    `h_host_draw`,
                    `h_host_lose`,
                    `h_host_count`,
                    `h_abs_gs`,
                    `h_abs_gd`,
                    `h_abs_g`,
                    `h_abs_avg_gs`,
                    `h_abs_avg_gd`,
                    `h_abs_avg_g`,
                    `h_host_gs`,
                    `h_host_gd`,
                    `h_host_g`,
                    `h_0_1_gs`,
                    `h_2_3_gs`,
                    `h_ab_4_gs`,
                    `h_0_gs`,
                    `h_1_gs`,
                    `h_2_gs`,
                    `h_3_gs`,
                    `h_4_gs`,
                    `h_5_gs`,
                    `h_6_gs`,
                    `h_7_gs`,
                    `h_0_1_gd`,
                    `h_2_3_gd`,
                    `h_ab_4_gd`,
                    `h_0_gd`,
                    `h_1_gd`,
                    `h_2_gd`,
                    `h_3_gd`,
                    `h_4_gd`,
                    `h_5_gd`,
                    `h_6_gd`,
                    `h_7_gd`,
                    `v_abs_win`,
                    `v_abs_draw`,
                    `v_abs_lose`,
                    `v_count`,
                    `v_visit_win`,
                    `v_visit_draw`,
                    `v_visit_lose`,
                    `v_visit_count`,
                    `v_abs_gs`,
                    `v_abs_gd`,
                    `v_abs_g`,
                    `v_abs_avg_gs`,
                    `v_abs_avg_gd`,
                    `v_abs_avg_g`,
                    `v_visit_gs`,
                    `v_visit_gd`,
                    `v_visit_g`,
                    `v_0_1_gs`,
                    `v_2_3_gs`,
                    `v_ab_4_gs`,
                    `v_0_gs`,
                    `v_1_gs`,
                    `v_2_gs`,
                    `v_3_gs`,
                    `v_4_gs`,
                    `v_5_gs`,
                    `v_6_gs`,
                    `v_7_gs`,
                    `v_0_1_gd`,
                    `v_2_3_gd`,
                    `v_ab_4_gd`,
                    `v_0_gd`,
                    `v_1_gd`,
                    `v_2_gd`,
                    `v_3_gd`,
                    `v_4_gd`,
                    `v_5_gd`,
                    `v_6_gd`,
                    `v_7_gd`
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                """

                cursor.execute(sql, (
                    str(item["matchid"]),
                    str(item["h_abs_win"]),
                    str(item["h_abs_draw"]),
                    str(item["h_abs_lose"]),
                    str(item["h_count"]),
                    str(item["h_host_win"]),
                    str(item["h_host_draw"]),
                    str(item["h_host_lose"]),
                    str(item["h_host_count"]),
                    str(item["h_abs_gs"]),
                    str(item["h_abs_gd"]),
                    str(item["h_abs_g"]),
                    str(item["h_abs_avg_gs"]),
                    str(item["h_abs_avg_gd"]),
                    str(item["h_abs_avg_g"]),
                    str(item["h_host_gs"]),
                    str(item["h_host_gd"]),
                    str(item["h_host_g"]),
                    # str(item["h_host_avg_gs"]),
                    # str(item["h_host_avg_gd"]),
                    # str(item["h_host_avg_g"]),
                    str(item["h_0_1_gs"]),
                    str(item["h_2_3_gs"]),
                    str(item["h_ab_4_gs"]),
                    str(item["h_0_gs"]),
                    str(item["h_1_gs"]),
                    str(item["h_2_gs"]),
                    str(item["h_3_gs"]),
                    str(item["h_4_gs"]),
                    str(item["h_5_gs"]),
                    str(item["h_6_gs"]),
                    str(item["h_7_gs"]),
                    str(item["h_0_1_gd"]),
                    str(item["h_2_3_gd"]),
                    str(item["h_ab_4_gd"]),
                    str(item["h_0_gd"]),
                    str(item["h_1_gd"]),
                    str(item["h_2_gd"]),
                    str(item["h_3_gd"]),
                    str(item["h_4_gd"]),
                    str(item["h_5_gd"]),
                    str(item["h_6_gd"]),
                    str(item["h_7_gd"]),
                    str(item["v_abs_win"]),
                    str(item["v_abs_draw"]),
                    str(item["v_abs_lose"]),
                    str(item["v_count"]),
                    str(item["v_visit_win"]),
                    str(item["v_visit_draw"]),
                    str(item["v_visit_lose"]),
                    str(item["v_visit_count"]),
                    str(item["v_abs_gs"]),
                    str(item["v_abs_gd"]),
                    str(item["v_abs_g"]),
                    str(item["v_abs_avg_gs"]),
                    str(item["v_abs_avg_gd"]),
                    str(item["v_abs_avg_g"]),
                    str(item["v_visit_gs"]),
                    str(item["v_visit_gd"]),
                    str(item["v_visit_g"]),
                    # str(item["v_visit_avg_gs"]),
                    # str(item["v_visit_avg_gd"]),
                    # str(item["v_visit_avg_g"]),
                    str(item["v_0_1_gs"]),
                    str(item["v_2_3_gs"]),
                    str(item["v_ab_4_gs"]),
                    str(item["v_0_gs"]),
                    str(item["v_1_gs"]),
                    str(item["v_2_gs"]),
                    str(item["v_3_gs"]),
                    str(item["v_4_gs"]),
                    str(item["v_5_gs"]),
                    str(item["v_6_gs"]),
                    str(item["v_7_gs"]),
                    str(item["v_0_1_gd"]),
                    str(item["v_2_3_gd"]),
                    str(item["v_ab_4_gd"]),
                    str(item["v_0_gd"]),
                    str(item["v_1_gd"]),
                    str(item["v_2_gd"]),
                    str(item["v_3_gd"]),
                    str(item["v_4_gd"]),
                    str(item["v_5_gd"]),
                    str(item["v_6_gd"]),
                    str(item["v_7_gd"]),
                ))

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

        elif isinstance(item, FSpiderOddInfo):
            with self.connection.cursor() as cursor:
                sql = """
                    INSERT INTO `breadt_match_odd_info` (`matchid`,`avg_init_win_odd`, `avg_init_draw_odd`, `avg_init_lose_odd`, `avg_new_win_odd`, `avg_new_draw_odd`, `avg_new_lose_odd`, `avg_new_win_rate`, `avg_new_draw_rate`, `avg_new_lose_rate`, `avg_new_win_kelly`, `avg_new_draw_kelly`, `avg_new_lose_kelly`, `avg_pay_rate`,`max_init_win_odd`, `max_init_draw_odd`, `max_init_lose_odd`, `max_new_win_odd`, `max_new_draw_odd`, `max_new_lose_odd`, `max_new_win_rate`, `max_new_draw_rate`, `max_new_lose_rate`, `max_new_win_kelly`, `max_new_draw_kelly`, `max_new_lose_kelly`, `max_pay_rate`,`min_init_win_odd`, `min_init_draw_odd`, `min_init_lose_odd`, `min_new_win_odd`, `min_new_draw_odd`, `min_new_lose_odd`, `min_new_win_rate`, `min_new_draw_rate`, `min_new_lose_rate`, `min_new_win_kelly`, `min_new_draw_kelly`, `min_new_lose_kelly`, `min_pay_rate`,`std_win`, `std_draw`, `std_lose`, `dispersion_win`, `dispersion_draw`, `dispersion_lose`
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s
                    );
                """
                # sql = "INSERT INTO `breadt_match_odd_info` (`matchid`, `odd_type`, `init_win_odd`, `init_draw_odd`, `init_lose_odd`, `new_win_odd`, `new_draw_odd`, `new_lose_odd`, `new_win_rate`, `new_draw_rate`, `new_lose_rate`, `new_win_kelly`, `new_draw_kelly`, `new_lose_kelly`, `pay_rate`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                cursor.execute(sql, (
                    item['matchid'],
                    item['avg_init_win_odd'], item['avg_init_draw_odd'], item['avg_init_lose_odd'], item['avg_new_win_odd'], item['avg_new_draw_odd'], item['avg_new_lose_odd'], item['avg_new_win_rate'], item['avg_new_draw_rate'], item['avg_new_lose_rate'], item['avg_new_win_kelly'], item['avg_new_draw_kelly'], item['avg_new_lose_kelly'], item['avg_pay_rate'],
                    item['max_init_win_odd'], item['max_init_draw_odd'], item['max_init_lose_odd'], item['max_new_win_odd'], item['max_new_draw_odd'], item['max_new_lose_odd'], item['max_new_win_rate'], item['max_new_draw_rate'], item['max_new_lose_rate'], item['max_new_win_kelly'], item['max_new_draw_kelly'], item['max_new_lose_kelly'], item['max_pay_rate'],
                    item['min_init_win_odd'], item['min_init_draw_odd'], item['min_init_lose_odd'], item['min_new_win_odd'], item['min_new_draw_odd'], item['min_new_lose_odd'], item['min_new_win_rate'], item['min_new_draw_rate'], item['min_new_lose_rate'], item['min_new_win_kelly'], item['min_new_draw_kelly'], item['min_new_lose_kelly'], item['min_pay_rate'],
                    item['std_win'], item['std_draw'], item['std_lose'], item['dispersion_win'], item['dispersion_draw'], item['dispersion_lose']
                ))

            self.connection.commit()
        
        elif isinstance(item, FSpiderOffsetOddInfo):
            with self.connection.cursor() as cursor:
                sql = " INSERT INTO `breadt_football_offset_info` (`matchid`,  `company`, `init_offset`, `init_host`,`init_visit`, `new_offset`,`new_host`,`new_visit`, `new_host_rate`, `new_visit_rate`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                cursor.execute(sql, (
                    item['matchid'], item['company'],
                    item['init_offset'], item['init_host'], item['init_visit'], 
                    item['new_offset'], item['new_host'], item['new_visit'], 
                    item['new_host_rate'], item['new_visit_rate']
                ))

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
