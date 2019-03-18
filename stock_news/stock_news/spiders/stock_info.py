# -*- coding: utf-8 -*-
import scrapy
import pandas as pd


class StockInfoSpider(scrapy.Spider):
    name = 'stock_info'
    allowed_domains = ['sina.com']
    start_urls = ['http://sina.com/']

    domain = 'http://vip.stock.finance.sina.com.cn/corp/view/vCB_AllNewsStock.php?symbol=sz002363&Page=2'

    def start_requests(self):
    	df = pd.read_pickle('../data/s.list.pkl')

    	for index, row in df.iterrows():
    		if row['code'].startswith('00', 0, 1):
    			yield self.request_one_page('sz'+row['code'], 1)
    		elif row['code'].startswith('30', 0, 1):
    			yield self.request_one_page('sz'+row['code'], 1)
    		elif row['code'].startswith('60', 0, 1):
    			yield self.request_one_page('sh'+row['code'], 1)


    def request_one_page(self, symbol, page):
        url = self.domain + '?' + urlencode({'symbol': symbol, 'Page': page})
        return scrapy.Request(url=url,
                              callback=self.parse,
                              meta={"code": symbol, 'page': page})

    def parse(self, response):
        pass
