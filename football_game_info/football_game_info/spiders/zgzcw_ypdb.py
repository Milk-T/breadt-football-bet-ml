# -*- coding: utf-8 -*-
import scrapy
import pymysql.cursors
from ..items import FSpiderOffsetOddInfo


class ZgzcwYpdbSpider(scrapy.Spider):
    name = 'zgzcw_ypdb'
    allowed_domains = ['zgzcw.com']
    start_urls = ['http://zgzcw.com/']

    domain = 'http://fenxi.zgzcw.com/%d/ypdb'

    def start_requests(self):

        connection = pymysql.connect(host='localhost', user='root', password='breadt@2019', db='breadt-football-ml', charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)

        with connection.cursor() as cursor:
            sql = 'select matchid from `breadt_football_game_list` where matchid not in (select matchid from `breadt_football_offset_info`);'
            cursor.execute(sql)
            rows = cursor.fetchall()

            for row in rows:
                yield scrapy.Request(
                    url=self.domain % (row['matchid']),
                    callback=self.parse,
                    meta={'matchid': row['matchid']}
            )

        # yield scrapy.Request(
        #     url=self.domain % (2503002),
        #     callback=self.parse,
        #     meta={'matchid': 2503002}
        # )

    def parse(self, response):
        tr = response.xpath('(.//div[@id="data-body"]//tr)[1]')
        if tr is None:
            return

        tds = tr.xpath('.//td')

        yield FSpiderOffsetOddInfo(
            matchid = response.meta['matchid'],
            company = tds[1].xpath('.//text()').extract_first(),
            init_offset = tds[3].xpath('.//text()').extract_first(),
            init_host = tds[2].xpath('.//text()').extract_first(),
            init_visit = tds[4].xpath('.//text()').extract_first(),

            new_offset = tds[6].xpath('.//a/text()').extract_first(),
            new_host = tds[5].xpath('.//a/text()').extract_first().replace('↑', '').replace('↓', ''),
            new_visit = tds[7].xpath('.//a/text()').extract_first().replace('↑', '').replace('↓', ''),
            new_host_rate = tds[9].xpath('.//text()').extract_first(),
            new_visit_rate = tds[10].xpath('.//text()').extract_first()
        )
