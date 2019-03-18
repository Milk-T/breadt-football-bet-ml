# -*- coding: utf-8 -*-
import scrapy
import pandas as pd


class StockInfoSpider(scrapy.Spider):
    name = 'stock_info'
    allowed_domains = ['sina.com']
    start_urls = ['http://sina.com/']

    domain = 'http://vip.stock.finance.sina.com.cn/corp/view/vCB_AllNewsStock.php?symbol=sz002363&Page=2'

    def start_requests(self):
        pass

    def request_one_page(self, symbol, page):
        url = self.domain + '?' + urlencode({'symbol': symbol, 'Page': page})
        return scrapy.Request(url=url,
                              callback=self.parse,
                              meta={"code": symbol})

    def parse(self, response):
        pass
