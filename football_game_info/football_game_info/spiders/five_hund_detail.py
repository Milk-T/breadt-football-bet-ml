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
        df = df.read_pickle('../data/f.brief.test.pkl')
        for index, row in df.iterrows():
            yield scrapy.Request(url=self.domain  % int(row['fid']), callback=self.parse)

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
        scores = tr.xpath('(.//td)[3]//em').extract()
        for ele in scores:
            if not isinstance(ele, str):
                score = score + ele.contents[0]
            else:
                score = score + ele
        arr = score.split(':')

        yield FSpiderReferInfo(
            fid=fid,
            pos=pos,
            name=tr.xpath('(.//td)[1]/a/text()').extract_first(),
            date=prefix + tr.xpath('(.//td)[2]/text()').extract_first(),
            host_team=tr.xpath('(.//td)[3]/span[@class=dz-l]/text()').extract_first(),
            visit_team=tr.xpath('(.//td)[3]/span[@class=dz-r]/text()').extract_first(),
            gs=int(arr[0]),
            gd=int(arr[1]),
            result=self._get_result(arr),
            gn=int(arr[0]) + int(arr[1])
        )

    def _fetch_form(self, response, fid, pos, name):
        trs = response.xpath('(.//form[@name=%s])[0]/tbody//tr' % (name))

        for tr in trs.xpath('[@class=tr1]').extract()
            _fetch_one(tr, fid, pos)

        for tr in trs.xpath('[@class=tr2]').extract()
            _fetch_one(tr, fid, pos)

    def _fetch_ex(self, tr, fid, pos, prefix='20'):
        score = ''
        scores = tr.xpath('(.//td)[3]//em').extract()
        for ele in scores:
            if not isinstance(ele, str):
                score = score + ele.contents[0]
            else:
                score = score + ele
        arr = score.split(':')

        yield FSpiderReferInfo(
            fid=fid,
            pos=pos,
            name=tr.xpath('(.//td)[1]/a/text()').extract_first(),
            date=prefix + tr.xpath('(.//td)[2]/text()').extract_first(),
            host_team=self._get_str(tr.xpath('(.//td)[3]/span[@class=dz-l]/text()').extract()),
            visit_team=self._get_str(tr.xpath('(.//td)[3]/span[@class=dz-r]/text()').extract()),
            gs=int(arr[0]),
            gd=int(arr[1]),
            result=self._get_result(arr),
            gn=int(arr[0]) + int(arr[1])
        )


    def parse(self, response):
        print(response.url)

        self._fetch_form(response, fid, 'hr10g', 'zhanji_01') # 10
        self._fetch_form(response, fid, 'vr10g', 'zhanji_00') # 10
        self._fetch_form(response, fid, 'hrhg', 'zhanji_11') # 10
        self._fetch_form(response, fid, 'vrvg', 'zhanji_20') # 10

        ex_games = [] # 3

        element = response.xpath('.//div[@id=team_jiaozhan]//table')

        if element is not None:
            trs = element.xpath('.//tr')

            for tr in trs.xpath('[@class=tr1]').extract()
                self._fetch_ex(tr, fid, 'ex', prefix='')

            for tr in trs.xpath('[@class=tr2]').extract()
                self._fetch_ex(tr, fid, 'ex', prefix='')

