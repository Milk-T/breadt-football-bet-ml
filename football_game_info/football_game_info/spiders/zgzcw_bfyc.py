# -*- coding: utf-8 -*-
import scrapy
import pymysql.cursors
from ..utils import take_result
import re


class ZgzcwBfycSpider(scrapy.Spider):
    name = 'zgzcw_bfyc'
    allowed_domains = ['fenxi.zgzcw.com']
    start_urls = ['http://zgzcw.com/']

    domain = "http://fenxi.zgzcw.com/%d/bfyc"


    def start_requests(self):

        connection = pymysql.connect(host='localhost', user='root', password='breadt@2019', db='breadt-football-ml', charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)

        with connection.cursor() as cursor:
            # sql = 'select matchid from `breadt_football_game_list` where matchid not in (select matchid from `breadt_football_feature_info`);'
            sql = 'select matchid from `breadt_football_game_list` limit 1;'
            cursor.execute(sql)
            rows = cursor.fetchall()

            for row in rows:
                yield scrapy.Request(
                    url=self.domain % (row['matchid']),
                    callback=self.parse,
                    meta={'matchid': row['matchid']}
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

    def parse(self, response):
        trs = response.xpath('.//div[@id="hostList"]//tbody//tr')

        host_list = []
        for tr in trs:
            item = self.get_item(tr)

            if item['matchid'] != response.meta['matchid']:
                host_list.append(item)
            
            if len(host_list) == 10:
                break

        trs = response.xpath('.//div[@id="guestList"]//tbody//tr')

        visit_list = []
        for tr in trs:
            item = self.get_item(tr)

            if item['matchid'] != response.meta['matchid']:
                visit_list.append(item)
            
            if len(visit_list) == 10:
                break
        
