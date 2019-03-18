# -*- coding: utf-8 -*-
import scrapy
import pandas as pd
from ..items import FSpiderReferInfo


class FiveHundDetailSpider(scrapy.Spider):
    name = 'five_hund_detail'
    allowed_domains = ['500.com']
    start_urls = ['http://500.com/']

    domain = 'https://odds.500.com/fenxi/shuju-%d.shtml'

    def start_requests(self):
        df = pd.read_csv('../data/breadt_football_game_diff.csv')
        for index, row in df.iterrows():
            yield scrapy.Request(url=self.domain  % int(row['fid']), callback=self.parse, meta={'fid': int(row['fid'])})

        # df = pd.read_pickle('../data/f.brief.pkl')
        # detail = pd.read_pickle('../data/f.refer.pkl')
        # for index, row in df.iterrows():
        #     if len(detail[detail['fid'] == row['fid']]) == 0:
        #         yield scrapy.Request(url=self.domain  % int(row['fid']), callback=self.parse, meta={'fid': int(row['fid'])})

    def _get_result(self, scores):
        if scores[0] > scores[1]:
            return 2
        elif scores[0] == scores[1]:
            return 1
        else:
            return 0

    def _get_str(self, contents):
        for content in contents:
            if isinstance(content, str):
                return content

    def _fetch_one(self, tr, fid, pos, prefix='20'):
        score = ''
        scores = tr.xpath('(.//td)[3]//em//text()').extract()

        for ele in scores:
            score = score + ele

        arr = score.split(':')
        item = FSpiderReferInfo(
            fid=fid,
            pos=pos,
            name=tr.xpath('(.//td)[1]/a/text()').extract_first(),
            date=prefix + tr.xpath('(.//td)[2]/text()').extract_first(),
            host_team=tr.xpath('(.//td)[3]//span[contains(@class, "dz-l")]/text()').extract_first(),
            visit_team=tr.xpath('(.//td)[3]//span[contains(@class, "dz-r")]/text()').extract_first(),
            gs=int(arr[0]),
            gd=int(arr[1]),
            gn=int(arr[0]) + int(arr[1]),
            result=self._get_result(arr),
        )

        return item

    def _fetch_form(self, response, fid, pos, name):
        arr = []
        for tr in response.xpath('(.//form[@name="%s"])[1]//tbody//tr[@class="tr1"]' % (name)):
            arr.append(self._fetch_one(tr, fid, pos))

        for tr in response.xpath('(.//form[@name="%s"])[1]//tbody//tr[@class="tr2"]' % (name)):
            arr.append(self._fetch_one(tr, fid, pos))

        return arr

    def _fetch_ex(self, tr, fid, pos, prefix='20'):
        score = ''
        scores = tr.xpath('(.//td)[3]//em//text()').extract()

        for ele in scores:
            score = score + ele
        arr = score.split(':')

        item =  FSpiderReferInfo(
            fid=fid,
            pos=pos,
            name=tr.xpath('(.//td)[1]/a/text()').extract_first(),
            date=prefix + tr.xpath('(.//td)[2]/text()').extract_first(),
            host_team=tr.xpath('(.//td)[3]//span[contains(@class, "dz-l")]/text()').extract_first(),
            visit_team=tr.xpath('(.//td)[3]//span[contains(@class, "dz-r")]/text()').extract_first(),
            gs=int(arr[0]),
            gd=int(arr[1]),
            result=self._get_result(arr),
            gn=int(arr[0]) + int(arr[1])
        )

        return item


    def parse(self, response):
        print(response.url)

        fid = response.meta['fid']

        result = []
        result = result + self._fetch_form(response, fid, 'hr10g', 'zhanji_01') # 10
        result = result + self._fetch_form(response, fid, 'vr10g', 'zhanji_00') # 10
        result = result + self._fetch_form(response, fid, 'hrhg', 'zhanji_11') # 10
        result = result + self._fetch_form(response, fid, 'vrvg', 'zhanji_20') # 10

        ex_games = [] # 3

        element = response.xpath('.//div[@id="team_jiaozhan"]//table')

        if element is not None:
            for tr in element.xpath('.//tr[@class="tr1"]'):
                result.append(self._fetch_ex(tr, fid, 'ex', prefix=''))

            for tr in element.xpath('.//tr[@class="tr2"]'):
                result.append(self._fetch_ex(tr, fid, 'ex', prefix=''))

        for item in result:
            yield item
