# -*- coding: utf-8 -*-
import scrapy
import pandas as pd
from ..items import FSpiderFeatureInfo
import pymysql.cursors

class ZgzcwLotteryFeatureInfoSpider(scrapy.Spider):
    name = 'zgzcw_lottery_feature_info'
    allowed_domains = ['zgzcw.com']
    start_urls = ['http://zgzcw.com/']

    domain = 'http://fenxi.zgzcw.com/%d/zjtz'

    def start_requests(self):
        # matchid = 2406910
        # yield scrapy.Request(url=self.domain % (matchid),callback=self.parse,meta={'matchid': matchid})

        connection = pymysql.connect(host='localhost', user='root', password='breadt@2019', db='breadt-football-ml', charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)

        with connection.cursor() as cursor:
            sql = 'select matchid from `breadt_lottery_info` where matchid not in (select matchid from `breadt_football_feature_info`);'
            cursor.execute(sql)
            rows = cursor.fetchall()

            for row in rows:

                matchid = abs(int(row['matchid']))

                yield scrapy.Request(
                    url=self.domain % (matchid),
                    callback=self.parse,
                    meta={'matchid': matchid}
                )

    def get_data(self, ele):
        return ele.xpath('span[@class="chang"]/text()').extract_first().replace('[', '').replace(']', '').replace('场', '')

    def parse(self, response):
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
