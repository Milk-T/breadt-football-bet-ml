# -*- coding: utf-8 -*-
import scrapy
import datetime
import json
from urllib.parse import urlencode
from ..items import FSpiderBriefInfo

class FiveHundSpider(scrapy.Spider):
    name = "five_hund"
    allowed_domains = ["500.com"]
    start_urls = ['http://live.500.com/']

    domain = 'http://live.500.com/'

    def start_requests(self):
        current_date = datetime.datetime.now()
        fork_date = datetime.datetime(2011, 1, 1, 0, 0, 0, 100000)
        date_list = []
        while((current_date - fork_date).days > 1):
            date_list.append(fork_date)
            fork_date = fork_date + datetime.timedelta(days=1)

        for bet_date in date_list:
            yield scrapy.Request(url=self.domain+'?'+ urlencode({'e': bet_date.strftime("%Y-%m-%d")}),
                                 callback=self.parse)

            
    def parse(self, response):
        print('start-parsing:'+ response.url)

        trs = response.xpath('(//table)[4]/tbody//tr')

        arr = []
        for tr in trs:

            tds = tr.xpath('.//td')

            if len(tds) < 5:
                continue

            status = tds[4].xpath('./span/text()').extract_first()

            if status is None or status != u'完':
                continue

            fid = tr.xpath('@fid').extract_first()

            game = tds[1].xpath('./a/text()').extract_first()
            turn = tds[2].xpath('./text()').extract_first()
            home_team = tds[5].xpath('./a/text()').extract_first()
            visit_team = tds[7].xpath('./a/text()').extract_first()

            a_group = tds[6].xpath('.//a')
            gs = a_group[0].xpath('./text()').extract_first()
            gd = a_group[2].xpath('./text()').extract_first()

            item = FSpiderBriefInfo(
                fid = int(fid),
                status = status,
                game = game,
                turn = turn,
                home_team = home_team,
                visit_team = visit_team,
                gs = int(gs),
                gd = int(gd),
                gn = int(gs)+int(gd),
                offset = a_group[1].xpath('./text()').extract_first(),
                time = tds[3].xpath('./text()').extract_first()
            )

            yield item

        print('end-parsing:'+ response.url + ':over')
