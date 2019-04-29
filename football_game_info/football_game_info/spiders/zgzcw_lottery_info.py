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

    def get_result(self, arr):
        if int(arr[0]) > int(arr[1]):
            return 3
        elif int(arr[0]) == int(arr[1]):
            return 1
        else:
            return 0

    def parse(self, response):
        body = response.body_as_unicode()
        if len(body) > 0:
            o_json = json.loads(body)

            for index, match in enumerate(o_json['matchInfo']):
                if len(match['zuizhongbifen']) <= 3:
                    return

                score_str = match['zuizhongbifen']
                score_arr = score_str.split(';')
                score = score_arr[index]
                score_one_arr = score.split('-')

                gn = -1
                gd = -1
                gs = -1

                if score_one_arr[0] != '':
                    gs = int(score_one_arr[0])

                if score_one_arr[1] != '':
                    gd = int(score_one_arr[1])

                gn = gd + gs

                bet_arr = match['europeSp'].split(' ')

                yield FSpiderLotteryInfo(
                    matchid = match['playId'],
                    status = "完成",
                    game = match['leageNameFull'],
                    turn = '',
                    home_team = match['hostNameFull'],
                    visit_team = match['guestNameFull'],
                    gs = gs,
                    gd = gd,
                    gn = gn,
                    time = match['gameStartDate'],
                    result = self.get_result([gs, gd]),
                    win_bet_return = bet_arr[0],
                    draw_bet_return = bet_arr[1],
                    lose_bet_return = bet_arr[2]
                )
