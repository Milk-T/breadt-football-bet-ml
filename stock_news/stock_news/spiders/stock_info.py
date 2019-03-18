# -*- coding: utf-8 -*-
import scrapy
import pandas as pd
from ..items import SSpiderInfoBriefInfo


class StockInfoSpider(scrapy.Spider):
    name = 'stock_info'
    allowed_domains = ['sina.com']
    start_urls = ['http://sina.com/']

    domain = 'http://vip.stock.finance.sina.com.cn/corp/view/vCB_AllNewsStock.php'

    def start_requests(self):
        df = pd.read_pickle('../data/s.list.pkl')
        print(df)

        for index, row in df.iterrows():

            print(row['code'])

            if row['code'].startswith('00', 0, 1):
                print('###################')
                yield self.request_one_page('sz' + row['code'], 1)
            elif row['code'].startswith('30', 0, 1):
                yield self.request_one_page('sz' + row['code'], 1)
            elif row['code'].startswith('60', 0, 1):
                yield self.request_one_page('sh' + row['code'], 1)

            break

    def request_one_page(self, symbol, page):
        url = self.domain + '?' + urlencode({'symbol': symbol, 'Page': page})
        yield scrapy.Request(url=url,
                              callback=self.parse,
                              meta={"code": symbol, 'page': page})

    def parse(self, response):

        div = response.xpath('.//div[@class="datelist"]//tr')
        if len(div) > 0:
            links = div[0].xpath('.//ul//a')

            for link in links:
                url = link.xpath('@href').extract_first()
                group = url.split('/')
                yield SSpiderInfoBriefInfo(
                    link=url,
                    title=link.xpath('./text()').extract_first(),
                    date=group[5]
                )
