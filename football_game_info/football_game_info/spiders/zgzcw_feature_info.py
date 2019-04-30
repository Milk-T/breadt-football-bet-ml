# -*- coding: utf-8 -*-
import scrapy
import pandas as pd
from ..items import FSpiderFeatureInfo
import pymysql.cursors
from ..utils import is_blank


class ZgzcwFeatureInfoSpider(scrapy.Spider):
    """
    增量获取竞彩数据的特征数据
    """

    name = 'zgzcw_feature_info'
    allowed_domains = ['zgzcw.com']
    start_urls = ['http://zgzcw.com/']

    domain = 'http://fenxi.zgzcw.com/%d/zjtz'

    def start_requests(self):

        connection = pymysql.connect(host='localhost', user='root', password='breadt@2019', db='breadt-football-ml', charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)

        with connection.cursor() as cursor:
            sql = 'select matchid from `breadt_football_game_list` where matchid not in (select matchid from `breadt_football_feature_info`);'
            cursor.execute(sql)
            rows = cursor.fetchall()

            for row in rows:
                yield scrapy.Request(
                    url=self.domain % (row['matchid']),
                    callback=self.parse,
                    meta={'matchid': row['matchid']}
            )

    def get_data(self, ele):
        num = ele.xpath('span[@class="chang"]/text()').extract_first().replace('[', '').replace(']', '').replace('场', '')
        if is_blank(num):
            return 0

        return num

    def parse(self, response):
        # h_rank = response.xpath(
        #     './/div[@class="team-add-info-zd"]/text()').extract_first()

        # h_rank_arr = h_rank.split('  ')
        # if len(h_rank_arr) < 2:
        #     return

        # h_previous_rank = h_rank_arr[0].split('：')[1]
        # h_current_rank = h_rank_arr[1].split('：')[1]

        # v_rank = response.xpath(
        #     './/div[@class="team-add-info-kd"]/text()').extract_first()

        # v_rank_arr = v_rank.split('  ')
        # if len(v_rank_arr) < 2:
        #     return

        # v_previous_rank = v_rank_arr[0].split('：')[1]
        # v_current_rank = v_rank_arr[1].split('：')[1]

        h_containers = response.xpath(
            './/div[@class="zjtz-l"]//div[@class="marb10"]')

        v_containers = response.xpath(
            './/div[@class="zjtz-r"]//div[@class="marb10"]')

        yield FSpiderFeatureInfo(

            matchid=response.meta['matchid'],

            h_score = response.xpath('.//div[@class="team-info-h"]/text()').extract_first().replace("\r", "").replace("\t", "").replace("\n", "").strip(),
            v_score = response.xpath('.//div[@class="team-info-v"]/text()').extract_first().replace("\r", "").replace("\t", "").replace("\n", "").strip(),

            h_rank = response.xpath('.//div[@class="team-add-info-zd"]/text()').extract_first(),
            v_rank = response.xpath('.//div[@class="team-add-info-kd"]/text()').extract_first(),
            # h_pervious_rank=h_previous_rank,
            # h_current_rank=h_current_rank,
            # v_pervious_rank=v_previous_rank,
            # v_current_rank=v_current_rank,

            h_perf_win=self.get_data(h_containers[0].xpath('.//li')[0]),
            h_perf_draw=self.get_data(h_containers[0].xpath('.//li')[1]),
            h_perf_lose=self.get_data(h_containers[0].xpath('.//li')[2]),

            h_host_win=self.get_data(h_containers[0].xpath('.//li')[3]),
            h_host_draw=self.get_data(h_containers[0].xpath('.//li')[4]),
            h_host_lose=self.get_data(h_containers[0].xpath('.//li')[5]),

            h_battle_with_front_10_win=self.get_data(
                h_containers[0].xpath('.//li')[6]),
            h_battle_with_front_10_draw=self.get_data(
                h_containers[0].xpath('.//li')[7]),
            h_battle_with_front_10_lose=self.get_data(
                h_containers[0].xpath('.//li')[8]),

            h_battle_with_end_10_win=self.get_data(
                h_containers[0].xpath('.//li')[9]),
            h_battle_with_end_10_draw=self.get_data(
                h_containers[0].xpath('.//li')[10]),
            h_battle_with_end_10_lose=self.get_data(
                h_containers[0].xpath('.//li')[11]),

            h_perf_gs=self.get_data(h_containers[1].xpath('.//li')[0]),
            h_perf_gd=self.get_data(h_containers[1].xpath('.//li')[1]),
            h_perf_avg_gs=self.get_data(h_containers[1].xpath('.//li')[2]),
            h_perf_avg_gd=self.get_data(h_containers[1].xpath('.//li')[3]),
            h_host_gs=self.get_data(h_containers[1].xpath('.//li')[4]),
            h_host_gd=self.get_data(h_containers[1].xpath('.//li')[5]),
            h_host_avg_gs=self.get_data(h_containers[1].xpath('.//li')[6]),
            h_host_avg_gd=self.get_data(h_containers[1].xpath('.//li')[7]),
            h_r3_gs=self.get_data(h_containers[1].xpath('.//li')[8]),
            h_r3_gd=self.get_data(h_containers[1].xpath('.//li')[9]),
            h_r3_avg_gs=self.get_data(h_containers[1].xpath('.//li')[10]),
            h_r3_avg_gd=self.get_data(h_containers[1].xpath('.//li')[11]),

            h_perf_bet_high=self.get_data(h_containers[2].xpath('.//li')[0]),
            h_perf_bet_low=self.get_data(h_containers[2].xpath('.//li')[1]),
            h_host_bet_high=self.get_data(h_containers[2].xpath('.//li')[2]),
            h_host_bet_low=self.get_data(h_containers[2].xpath('.//li')[3]),

            h_host_0_1_goal=self.get_data(h_containers[3].xpath('.//li')[0]),
            h_host_2_3_goal=self.get_data(h_containers[3].xpath('.//li')[1]),
            h_host_ab_4_goal=self.get_data(h_containers[3].xpath('.//li')[2]),
            h_host_0_goal=self.get_data(h_containers[3].xpath('.//li')[3]),
            h_host_1_goal=self.get_data(h_containers[3].xpath('.//li')[4]),
            h_host_2_goal=self.get_data(h_containers[3].xpath('.//li')[5]),
            h_host_3_goal=self.get_data(h_containers[3].xpath('.//li')[6]),
            h_host_4_goal=self.get_data(h_containers[3].xpath('.//li')[7]),
            h_host_5_goal=self.get_data(h_containers[3].xpath('.//li')[8]),
            h_host_6_goal=self.get_data(h_containers[3].xpath('.//li')[9]),
            h_host_7_goal=self.get_data(h_containers[3].xpath('.//li')[10]),

            v_perf_win=self.get_data(v_containers[0].xpath('.//li')[0]),
            v_perf_draw=self.get_data(v_containers[0].xpath('.//li')[1]),
            v_perf_lose=self.get_data(v_containers[0].xpath('.//li')[2]),

            v_host_win=self.get_data(v_containers[0].xpath('.//li')[3]),
            v_host_draw=self.get_data(v_containers[0].xpath('.//li')[4]),
            v_host_lose=self.get_data(v_containers[0].xpath('.//li')[5]),

            v_battle_with_front_10_win=self.get_data(
                v_containers[0].xpath('.//li')[6]),
            v_battle_with_front_10_draw=self.get_data(
                v_containers[0].xpath('.//li')[7]),
            v_battle_with_front_10_lose=self.get_data(
                v_containers[0].xpath('.//li')[8]),

            v_battle_with_end_10_win=self.get_data(
                v_containers[0].xpath('.//li')[9]),
            v_battle_with_end_10_draw=self.get_data(
                v_containers[0].xpath('.//li')[10]),
            v_battle_with_end_10_lose=self.get_data(
                v_containers[0].xpath('.//li')[11]),

            v_perf_gs=self.get_data(v_containers[1].xpath('.//li')[0]),
            v_perf_gd=self.get_data(v_containers[1].xpath('.//li')[1]),
            v_perf_avg_gs=self.get_data(v_containers[1].xpath('.//li')[2]),
            v_perf_avg_gd=self.get_data(v_containers[1].xpath('.//li')[3]),
            v_host_gs=self.get_data(v_containers[1].xpath('.//li')[4]),
            v_host_gd=self.get_data(v_containers[1].xpath('.//li')[5]),
            v_host_avg_gs=self.get_data(v_containers[1].xpath('.//li')[6]),
            v_host_avg_gd=self.get_data(v_containers[1].xpath('.//li')[7]),
            v_r3_gs=self.get_data(v_containers[1].xpath('.//li')[8]),
            v_r3_gd=self.get_data(v_containers[1].xpath('.//li')[9]),
            v_r3_avg_gs=self.get_data(v_containers[1].xpath('.//li')[10]),
            v_r3_avg_gd=self.get_data(v_containers[1].xpath('.//li')[11]),

            v_perf_bet_high=self.get_data(v_containers[2].xpath('.//li')[0]),
            v_perf_bet_low=self.get_data(v_containers[2].xpath('.//li')[1]),
            v_host_bet_high=self.get_data(v_containers[2].xpath('.//li')[2]),
            v_host_bet_low=self.get_data(v_containers[2].xpath('.//li')[3]),

            v_host_0_1_goal=self.get_data(v_containers[3].xpath('.//li')[0]),
            v_host_2_3_goal=self.get_data(v_containers[3].xpath('.//li')[1]),
            v_host_ab_4_goal=self.get_data(v_containers[3].xpath('.//li')[2]),
            v_host_0_goal=self.get_data(v_containers[3].xpath('.//li')[3]),
            v_host_1_goal=self.get_data(v_containers[3].xpath('.//li')[4]),
            v_host_2_goal=self.get_data(v_containers[3].xpath('.//li')[5]),
            v_host_3_goal=self.get_data(v_containers[3].xpath('.//li')[6]),
            v_host_4_goal=self.get_data(v_containers[3].xpath('.//li')[7]),
            v_host_5_goal=self.get_data(v_containers[3].xpath('.//li')[8]),
            v_host_6_goal=self.get_data(v_containers[3].xpath('.//li')[9]),
            v_host_7_goal=self.get_data(v_containers[3].xpath('.//li')[10]),
        )
