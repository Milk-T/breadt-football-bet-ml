# -*- coding: utf-8 -*-
import scrapy
import datetime
import json
from urllib.parse import urlencode
from ..items import FSpiderPredictInfo

class FiveHundPredictSpider(scrapy.Spider):
    name = 'five_hund_predict'
    allowed_domains = ['500.com']
    start_urls = ['http://500.com/']

    domain = 'http://live.500.com/'


    # 增量获取前1日数据，每天晚上5点运行
    def start_requests(self):
        current_date = datetime.datetime.now()

        # yield scrapy.Request(url=self.domain+'?'+ urlencode({'e': current_date.strftime("%Y-%m-%d")}),
        #                      callback=self.parse,
        #                      meta={"year": current_date.year})

        yield scrapy.Request(url=self.domain,
                             callback=self.parse,
                             meta={"year": current_date.year})

    def _get_result(self, gs, gd):
        if gs > gd:
            return 2
        elif gs == gd:
            return 1
        else:
            return 0

            
    def parse(self, response):
        print('start-parsing:'+ response.url)

        trs = response.xpath('(//table)[4]/tbody//tr')

        arr = []
        for tr in trs:

            tds = tr.xpath('.//td')

            if len(tds) < 5:
                continue

            status = tds[4].xpath('./text()').extract_first()

            if status is None or status != u'未':
                continue

            fid = tr.xpath('@fid').extract_first()

            game = tds[1].xpath('./a/text()').extract_first()
            turn = tds[2].xpath('./text()').extract_first()
            home_team = tds[5].xpath('./a/text()').extract_first()
            visit_team = tds[7].xpath('./a/text()').extract_first()

            a_group = tds[6].xpath('.//a')
            gs = a_group[0].xpath('./text()').extract_first()
            gd = a_group[2].xpath('./text()').extract_first()

            # 时间这里的设置上还有问题, 只取日期，不取时间
            game_date = str(response.meta['year']) + '-' + tds[3].xpath('./text()').extract_first() + ':00'
            game_date_arr = game_date.split(' ')

            item = FSpiderPredictInfo(
                fid = int(fid),
                status = status,
                game = game,
                turn = turn,
                home_team = home_team,
                visit_team = visit_team,
                offset = a_group[1].xpath('./text()').extract_first(),
                time = game_date_arr[0],
            )

            yield item

        print('end-parsing:'+ response.url + ':over')
