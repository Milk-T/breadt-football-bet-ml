# -*- coding: utf-8 -*-
import scrapy
import pandas as pd
from ..items import SSpiderInfoBriefInfo
from urllib.parse import urlencode
import re


class StockInfoSpider(scrapy.Spider):
    name = 'stock_info'
    allowed_domains = ['sina.com.cn']
    start_urls = ['http://sina.com.cn/', 'http://vip.stock.finance.sina.com.cn/']

    domain = 'http://vip.stock.finance.sina.com.cn/corp/view/vCB_AllNewsStock.php'

    def start_requests(self):
        df = pd.read_pickle('../data/s.list.pkl')

        for index, row in df.iterrows():
            if row['code'].startswith('00', 0, 2):
                yield self.request_one_page('sz' + row['code'], 1)
            elif row['code'].startswith('30', 0, 2):
                yield self.request_one_page('sz' + row['code'], 1)
            elif row['code'].startswith('60', 0, 2):
                yield self.request_one_page('sh' + row['code'], 1)
        # code = 'sz300641'
        # yield self.request_one_page(code, 1)

    def request_one_page(self, symbol, page):
        url = self.domain + '?' + urlencode({'symbol': symbol, 'Page': page})
        return scrapy.Request(url=url,
                              callback=self.parse,
                              meta={"code": symbol, 'page': page})

    def parse(self, response):
        div = response.xpath('.//div[@class="datelist"]')
        if len(div) > 0:

            links = div[0].xpath('.//ul//a')
            for link in links:
                url = link.xpath('@href').extract_first()

                search_result = re.search(r'\d+-\d+-\d+', url, re.M | re.I)
                if search_result:
                    date = search_result.group()
                else:
                    continue

                search_result = re.search( r'doc-\w+', url, re.M|re.I)
                signal = search_result.group()

                yield SSpiderInfoBriefInfo(
                    code=response.meta['code'],
                    link=url,
                    title=link.xpath('./text()').extract_first(),
                    date=date,
                    signal=signal
                )

            links = response.xpath('(.//table[@class="table2"]//div)[3]//a')
            if len(links) > 0:
                yield self.request_one_page(response.meta['code'], response.meta['page'] + 1)

            
