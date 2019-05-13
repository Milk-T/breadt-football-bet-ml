# -*- coding: utf-8 -*-
import scrapy
import pymysql.cursors
from ..utils import take_result
import re
import pandas as pd
from ..items import FSpiderRecentFeatureInfo


class ZgzcwBfycSpider(scrapy.Spider):
    name = 'zgzcw_bfyc'
    allowed_domains = ['fenxi.zgzcw.com']
    start_urls = ['http://zgzcw.com/']

    domain = "http://fenxi.zgzcw.com/%d/bfyc"

    def start_requests(self):

        connection = pymysql.connect(
            host='10.12.86.109',
            user='root',
            password='breadt@2019',
            db='breadt-football-ml',
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor)

        with connection.cursor() as cursor:
            # sql = 'select matchid from `breadt_football_game_list` where
            # matchid not in (select matchid from
            # `breadt_football_feature_info`);'
            sql = 'select matchid, home_team as host_team, visit_team from `breadt_football_game_list` where matchid not in (select matchid from `breadt_football_recent_feature_info`);'
            cursor.execute(sql)
            rows = cursor.fetchall()

            for row in rows:
                yield scrapy.Request(
                    url=self.domain % (row['matchid']),
                    callback=self.parse,
                    meta=row
            )

    def get_item(self, tr):
        tds = tr.xpath('.//td')
        scores = tds[4].xpath('.//a/strong/text()').extract_first().split(':')
        url = tds[4].xpath('.//a/@href').extract_first()

        item = {
            "game": tds[0].xpath('./text()').extract_first(),
            "host_team": tds[3].xpath('.//a/text()').extract_first(),
            "gs": int(scores[0]),
            "gd": int(scores[1]),
            "gn": int(scores[0]) + int(scores[1]),
            "result": take_result(int(scores[0]), int(scores[1])),
            "visit_team": tds[5].xpath('.//a/text()').extract_first(),
            "matchid": int(re.search(r'\d{7}', url).group(0))
        }

        return item

    def get_team_item(self, team, result, type, item):
        return {
            "team": team,
            "type": type,
            "gs": int(item['gs']),
            "gd": int(item['gd']),
            "gn": int(item['gn']),
            "result": result,
        }

    def get_opposit_result(self, result):
        if result == 0:
            return 2
        elif result == 2:
            return 0
        else:
            return result

    def get_record_list(self, trs, matchid, team):
        ts_list = []
        for tr in trs:
            item = self.get_item(tr)

            if item['matchid'] != matchid:
                if item['host_team'] == team:
                    ts_list.append(
                        self.get_team_item(
                            item['host_team'],
                            item['result'],
                            'host',
                            item))
                elif item['visit_team'] == team:
                    ts_list.append(
                        self.get_team_item(
                            item['host_team'],
                            self.get_opposit_result(
                                item['result']),
                                'visit',
                                item))

                ts_list.append(item)

            if len(ts_list) == 10:
                break

        return ts_list

    def parse(self, response):
        trs = response.xpath('.//div[@id="hostList"]//tbody//tr')
        host_list = self.get_record_list(trs, response.meta['matchid'], response.meta['host_team'])
        host_df = pd.DataFrame(host_list)

        trs = response.xpath('.//div[@id="guestList"]//tbody//tr')
        visit_list = self.get_record_list(trs, response.meta['matchid'], response.meta['visit_team'])
        visit_df = pd.DataFrame(visit_list)

        yield FSpiderRecentFeatureInfo(
            matchid=response.meta['matchid'],

            h_abs_win=len(host_df[host_df['result'] == 2]),
            h_abs_draw=len(host_df[host_df['result'] == 1]),
            h_abs_lose=len(host_df[host_df['result'] == 0]),
            h_count=len(host_df),
            h_host_win=len(host_df[ (host_df['result'] == 2) & (host_df['type'] == 'host') ]),
            h_host_draw=len( host_df[(host_df['result'] == 1) & (host_df['type'] == 'host') ]),
            h_host_lose=len( host_df[(host_df['result'] == 0) & (host_df['type'] == 'host') ]),
            h_host_count=len(host_df[host_df['type'] == 'host']),

            h_abs_gs = host_df['gs'].sum(),
            h_abs_gd = host_df['gd'].sum(),
            h_abs_g = host_df['gs'].sum() - host_df['gd'].sum(),
            h_abs_avg_gs = host_df['gs'].sum() / len(host_df),
            h_abs_avg_gd = host_df['gd'].sum() / len(host_df),
            h_abs_avg_g = (host_df['gs'].sum() -
                           host_df['gd'].sum()) / len(host_df),

            h_host_gs = host_df[host_df['type'] == 'host']['gs'].sum(),
            h_host_gd = host_df[host_df['type'] == 'host']['gd'].sum(),
            h_host_g = host_df[host_df['type'] == 'host']['gs'].sum() - host_df[host_df['type'] == 'host']['gd'].sum(),
            # h_host_avg_gs = host_df[host_df['type'] == 'host']['gs'].sum(
            # ) / len(host_df[host_df['type'] == 'host']) if len(host_df[host_df['type'] == 'host']) > 0 else 'NULL',
            # h_host_avg_gd = host_df[host_df['type'] == 'host']['gd'].sum(
            # ) / len(host_df[host_df['type'] == 'host'])  if len(host_df[host_df['type'] == 'host']) > 0 else 'NULL',
            # h_host_avg_g = (host_df[host_df['type'] == 'host']['gs'].sum() - host_df[host_df['type'] == 'host']['gd'].sum()) / len(host_df[host_df['type'] == 'host'])  if len(host_df[host_df['type'] == 'host']) > 0 else 'NULL',

            h_0_1_gs = len(host_df[host_df['gs'].isin([0, 1])]),
            h_2_3_gs = len(host_df[host_df['gs'].isin([2, 3])]),
            h_ab_4_gs = len(host_df[host_df['gs'] >= 4]),
            h_0_gs = len(host_df[host_df['gs'] == 0]),
            h_1_gs = len(host_df[host_df['gs'] == 1]),
            h_2_gs = len(host_df[host_df['gs'] == 2]),
            h_3_gs = len(host_df[host_df['gs'] == 3]),
            h_4_gs = len(host_df[host_df['gs'] == 4]),
            h_5_gs = len(host_df[host_df['gs'] == 5]),
            h_6_gs = len(host_df[host_df['gs'] == 6]),
            h_7_gs = len(host_df[host_df['gs'] >= 7]),

            h_0_1_gd = len(host_df[host_df['gd'].isin([0, 1])]),
            h_2_3_gd = len(host_df[host_df['gd'].isin([2, 3])]),
            h_ab_4_gd = len(host_df[host_df['gd'] >= 4]),
            h_0_gd = len(host_df[host_df['gd'] == 0]),
            h_1_gd = len(host_df[host_df['gd'] == 1]),
            h_2_gd = len(host_df[host_df['gd'] == 2]),
            h_3_gd = len(host_df[host_df['gd'] == 3]),
            h_4_gd = len(host_df[host_df['gd'] == 4]),
            h_5_gd = len(host_df[host_df['gd'] == 5]),
            h_6_gd = len(host_df[host_df['gd'] == 6]),
            h_7_gd = len(host_df[host_df['gd'] >= 7]),

            v_abs_win = len(visit_df[visit_df['result'] == 2]),
            v_abs_draw = len(visit_df[visit_df['result'] == 1]),
            v_abs_lose = len(visit_df[visit_df['result'] == 0]),
            v_count = len(visit_df),
            v_visit_win = len(visit_df[ (visit_df['result'] == 2) & (visit_df['type'] == 'visit')]),
            v_visit_draw = len(visit_df[ (visit_df['result'] == 1) & (visit_df['type'] == 'visit')]),
            v_visit_lose = len(visit_df[ (visit_df['result'] == 0) & (visit_df['type'] == 'visit')]),
            v_visit_count = len(visit_df[visit_df['type'] == 'visit']),

            v_abs_gs= visit_df['gs'].sum(),
            v_abs_gd= visit_df['gd'].sum(),
            v_abs_g= visit_df['gs'].sum() - visit_df['gd'].sum(),
            v_abs_avg_gs= visit_df['gs'].sum() / len(visit_df),
            v_abs_avg_gd= visit_df['gd'].sum() / len(visit_df),
            v_abs_avg_g= (visit_df['gs'].sum() - visit_df['gd'].sum()) / len(visit_df),

            v_visit_gs= visit_df[visit_df['type'] == 'visit']['gs'].sum(),
            v_visit_gd= visit_df[visit_df['type'] == 'visit']['gd'].sum(),
            v_visit_g= visit_df[visit_df['type'] == 'visit']['gs'].sum() - host_df[visit_df['type'] == 'visit']['gd'].sum(),
            # v_visit_avg_gs= visit_df[visit_df['type'] == 'visit']['gs'].sum() / len(visit_df[visit_df['type'] == 'visit']) if len(visit_df[visit_df['type'] == 'visit']) > 0 else 'NULL',
            # v_visit_avg_gd= visit_df[visit_df['type'] == 'visit']['gd'].sum() / len(visit_df[visit_df['type'] == 'visit']) if len(visit_df[visit_df['type'] == 'visit']) > 0 else 'NULL',
            # v_visit_avg_g= (visit_df[visit_df['type'] == 'visit']['gs'].sum() - visit_df[visit_df['type'] == 'visit']['gd'].sum()) / len(visit_df[visit_df['type'] == 'visit']) if len(visit_df[visit_df['type'] == 'visit']) > 0 else 'NULL',

            v_0_1_gs= len(visit_df[visit_df['gs'].isin([0, 1])]),
            v_2_3_gs= len(visit_df[visit_df['gs'].isin([2, 3])]),
            v_ab_4_gs= len(visit_df[visit_df['gs'] >= 4]),
            v_0_gs= len(visit_df[visit_df['gs'] == 0]),
            v_1_gs= len(visit_df[visit_df['gs'] == 1]),
            v_2_gs= len(visit_df[visit_df['gs'] == 2]),
            v_3_gs= len(visit_df[visit_df['gs'] == 3]),
            v_4_gs= len(visit_df[visit_df['gs'] == 4]),
            v_5_gs= len(visit_df[visit_df['gs'] == 5]),
            v_6_gs= len(visit_df[visit_df['gs'] == 6]),
            v_7_gs= len(visit_df[visit_df['gs'] >= 7]),

            v_0_1_gd= len(visit_df[visit_df['gd'].isin([0, 1])]),
            v_2_3_gd= len(visit_df[visit_df['gd'].isin([2, 3])]),
            v_ab_4_gd= len(visit_df[visit_df['gd'] >= 4]),
            v_0_gd= len(visit_df[visit_df['gd'] == 0]),
            v_1_gd= len(visit_df[visit_df['gd'] == 1]),
            v_2_gd= len(visit_df[visit_df['gd'] == 2]),
            v_3_gd= len(visit_df[visit_df['gd'] == 3]),
            v_4_gd= len(visit_df[visit_df['gd'] == 4]),
            v_5_gd= len(visit_df[visit_df['gd'] == 5]),
            v_6_gd= len(visit_df[visit_df['gd'] == 6]),
            v_7_gd= len(visit_df[visit_df['gd'] >= 7]),
        )
