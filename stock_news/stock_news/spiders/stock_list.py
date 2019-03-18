# -*- coding: utf-8 -*-
import scrapy
from ..items import SSpiderBriefInfo


class StockListSpider(scrapy.Spider):
    name = 'stock_list'
    allowed_domains = ['yz21.org']
    start_urls = ['http://yz21.org/']

    domain = 'http://yz21.org/stock/info/'

    def start_requests(self):
        yield scrapy.Request(url=self.domain, callback=self.parse)

        for i in range(2, 186, 1):
            yield scrapy.Request(url=self.domain + ('stocklist_%d.html' % (i)), callback=self.parse)

    def parse(self, response):
        trs = response.xpath('.//table[@id="All_stocks1_DataGrid1"]//tr')

        for i in range(1, len(trs), 1):
            tds = trs[i].xpath('.//td')

            item = SSpiderBriefInfo(
                code=tds[1].xpath('./a/text()').extract_first(),
                name=tds[2].xpath('./a/text()').extract_first(),
                full_name=tds[3].xpath('./a/text()').extract_first()
            )

            yield item
