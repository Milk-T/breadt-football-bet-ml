# -*- coding: utf-8 -*-
import scrapy
import pymysql.cursors
import time
import json
from ..items import FSpiderLotteryPredictInfo


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

            for index, match in enumerate(o_json['matchInfo']):
                if len(match['zuizhongbifen']) > 3:
                    return

                bet_arr = match['europeSp'].split(' ')
                print(bet_arr)
                if bet_arr[0] == '':
                    return

                spider = FSpiderLotteryPredictInfo(
                    matchid=abs(int(match['playId'].replace('-', ''))),
                    issue=match['issue'],
                    status="未开始",
                    game=match['leageName'].replace('　', ''),
                    turn='',
                    home_team=match['hostName'].replace('　', ''),
                    visit_team=match['guestName'].replace('　', ''),
                    time=match['gameStartDate'],
                    win_bet_return=bet_arr[0],
                    draw_bet_return=bet_arr[1],
                    lose_bet_return=bet_arr[2],
                )

                yield spider
