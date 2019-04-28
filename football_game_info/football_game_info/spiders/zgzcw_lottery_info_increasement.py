# -*- coding: utf-8 -*-
import scrapy
import pymysql.cursors
import time
import json
from ..items import FSpiderLotteryInfo


class ZgzcwLotteryInfoIncreasementSpider(scrapy.Spider):
    name = 'zgzcw_lottery_info_increasement'
    allowed_domains = ['zgzcw.com']
    start_urls = ['http://zgzcw.com/']

    domain = "http://cp.zgzcw.com/lottery/zcplayvs.action?lotteryId=13&issue=%d&v=%d"

    def start_requests(self):

        millis = int(round(time.time() * 1000))

        connection = pymysql.connect(host='localhost', user='root', password='breadt@2019',
                                     db='breadt-football-ml', charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)

        with connection.cursor() as cursor:
            sql = 'select max(issue) as max_issue from `breadt_lottery_info`;'
            cursor.execute(sql)
            row = cursor.fetchone()

            for i in range(int(row['max_issue']), 19201, 1):
                yield scrapy.Request(url=self.domain % (i, millis), callback=self.parse, meta={'issue': i})

    def parse(self, response):
        body = response.body_as_unicode()
        if len(body) > 0:
            o_json = json.loads(body)

            for match in o_json['matchInfo']:
                yield FSpiderLotteryInfo(
                    game_start_date=match['gameStartDate'],
                    issue=match['issue'],
                    matchid=match['playId'],
                )
