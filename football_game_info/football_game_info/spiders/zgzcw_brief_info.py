# -*- coding: utf-8 -*-
import scrapy
import datetime
from ..items import FSpiderBriefInfo


class ZgzcwBriefInfoSpider(scrapy.Spider):
    name = 'zgzcw_brief_info'
    allowed_domains = ['zgzcw.com']
    start_urls = ['http://zgzcw.com/']

    domain = 'http://live.zgzcw.com/ls/AllData.action'

    # 完全初始化 - 从2011年开始爬
    def start_requests(self):
        current_date = datetime.datetime.now()
        fork_date = datetime.datetime(2011, 4, 1, 0, 0, 0, 100000)
        # fork_date = datetime.datetime(2019, 4, 8, 0, 0, 0, 100000)

        date_list = []
        while((current_date - fork_date).days > 1):
            date_list.append(fork_date)
            fork_date = fork_date + datetime.timedelta(days=1)

        for bet_date in date_list:
            yield scrapy.FormRequest(
                url=self.domain,
                callback=self.parse,
                formdata={'code': '201', 'ajax': 'true',
                          'date': bet_date.strftime("%Y-%m-%d")}
            )

    def parse(self, response):
        trs = response.xpath('.//tr')

        for tr in trs:

            matchid = tr.xpath('@matchid').extract_first()

            tds = tr.xpath('.//td')

            status = tds[4].xpath('.//strong/text()').extract_first()

            if status is None or status.strip() != u'完':
                continue

            game = tds[1].xpath('.//span/text()').extract_first()
            turn = tds[2].xpath('./text()').extract_first()
            game_date = tds[3].xpath('@date').extract_first()

            goals_block = tds[6].xpath('.//span/text()').extract_first()
            goals = goals_block.split('-')

            gs = goals[0]
            gd = goals[1]

            home_team = tds[5].xpath('.//a/text()').extract_first()
            visit_team = tds[7].xpath('.//a/text()').extract_first()

            result = tds[9].xpath('(.//strong)[1]/text()').extract_first()

            spans = tds[10].xpath('(.//div)[3]//span')

            win_bet_return = spans[0].xpath('./text()').extract_first()
            draw_bet_return = spans[1].xpath('./text()').extract_first()
            lose_bet_return = spans[2].xpath('./text()').extract_first()

            item = FSpiderBriefInfo(
                matchid=int(matchid),
                status=status,
                game=game,
                turn=turn,
                home_team=home_team,
                visit_team=visit_team,
                gs=int(gs),
                gd=int(gd),
                gn=int(gs) + int(gd),
                time=game_date,
                result=result,
                win_bet_return=float(win_bet_return),
                draw_bet_return=float(draw_bet_return),
                lose_bet_return=float(lose_bet_return)
            )

            yield item
