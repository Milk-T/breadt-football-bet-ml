# -*- coding: utf-8 -*-
import scrapy
import time
import json
from ..items import FSpiderLotteryInfo


class ZgzcwLotteryInfoSpider(scrapy.Spider):
    name = 'zgzcw_lottery_info'
    allowed_domains = ['zgzcw.com']
    start_urls = ['http://zgzcw.com/']

    domain = "http://cp.zgzcw.com/lottery/zcplayvs.action?lotteryId=13&issue=%d&v=%d"

    def start_requests(self):
        millis = int(round(time.time() * 1000))

        for i in range(1, 201, 1):
            yield scrapy.Request(url=self.domain % (19000+i, millis), callback=self.parse, meta={'issue': 19000+i})

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
