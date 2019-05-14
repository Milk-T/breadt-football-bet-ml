# -*- coding: utf-8 -*-
import scrapy
from ..items import FSpiderOddInfo
import pymysql.cursors


class ZgzcwOddsInfoSpider(scrapy.Spider):
    name = 'zgzcw_odds_info'
    allowed_domains = ['zgzcw.com']
    start_urls = ['http://zgzcw.com/']

    domain = 'http://fenxi.zgzcw.com/%d/bjop'

    def start_requests(self):

        connection = pymysql.connect(host='localhost', user='root', password='breadt@2019',
                                     db='breadt-football-ai', charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)

        with connection.cursor() as cursor:
            sql = 'select matchid from `breadt_match_result_list` where matchid not in (select matchid from `breadt_match_odd_info`);'
            cursor.execute(sql)
            rows = cursor.fetchall()

            for row in rows:
                yield scrapy.Request(
                    url=self.domain % (row['matchid']),
                    callback=self.parse,
                    meta={'matchid': row['matchid']}
                )

    def get_content(self, ele, num):
        path = '(.//td)[%d]/text()' % num
        return ele.xpath(path).extract_first()

    # def get_item(self, tr, odd_type, matchid):
    #     return FSpiderOddInfo(
    #         odd_type=odd_type,
    #         matchid=matchid,
    #         init_win_odd=self.get_content(tr, 3),
    #         init_draw_odd=self.get_content(tr, 4),
    #         init_lose_odd=self.get_content(tr, 5),
    #         new_win_odd=tr.xpath('(.//td)[6]/a/text()').extract_first(),
    #         new_draw_odd=self.get_content(tr, 7),
    #         new_lose_odd=self.get_content(tr, 8),
    #         new_win_rate=self.get_content(tr, 10),
    #         new_draw_rate=self.get_content(tr, 11),
    #         new_lose_rate=self.get_content(tr, 12),
    #         new_win_kelly=self.get_content(tr, 13),
    #         new_draw_kelly=self.get_content(tr, 14),
    #         new_lose_kelly=self.get_content(tr, 15),
    #         pay_rate=self.get_content(tr, 16),
    #     )

    def parse(self, response):
        trs = response.xpath('.//div[@id="data-footer"]/table//tr')

        if len(trs) > 0 and float(self.get_content(trs[0], 3)) > 0:

            tr_0 = trs[0]
            tr_1 = trs[1]
            tr_2 = trs[2]
            tr_3 = trs[3]

            content = tr_3.xpath('.//span[@id="otherodds"]/text()').extract_first().replace('\n', '').replace('离散度%', '').replace('中足网方差%', '')
            arr = content.split('|')
            if len(arr) < 2:
                return
            
            dispersions = arr[0].strip().split(' ')
            stds = arr[1].strip().split(' ')

            yield FSpiderOddInfo(
                matchid=response.meta['matchid'],
                avg_init_win_odd=self.get_content(tr_0, 3),
                avg_init_draw_odd=self.get_content(tr_0, 4),
                avg_init_lose_odd=self.get_content(tr_0, 5),
                avg_new_win_odd=tr_0.xpath('(.//td)[6]/a/text()').extract_first(),
                avg_new_draw_odd=self.get_content(tr_0, 7),
                avg_new_lose_odd=self.get_content(tr_0, 8),
                avg_new_win_rate=self.get_content(tr_0, 10),
                avg_new_draw_rate=self.get_content(tr_0, 11),
                avg_new_lose_rate=self.get_content(tr_0, 12),
                avg_new_win_kelly=self.get_content(tr_0, 13),
                avg_new_draw_kelly=self.get_content(tr_0, 14),
                avg_new_lose_kelly=self.get_content(tr_0, 15),
                avg_pay_rate=self.get_content(tr_0, 16),

                max_init_win_odd=self.get_content(tr_1, 3),
                max_init_draw_odd=self.get_content(tr_1, 4),
                max_init_lose_odd=self.get_content(tr_1, 5),
                max_new_win_odd=tr_1.xpath('(.//td)[6]/a/text()').extract_first(),
                max_new_draw_odd=self.get_content(tr_1, 7),
                max_new_lose_odd=self.get_content(tr_1, 8),
                max_new_win_rate=self.get_content(tr_1, 10),
                max_new_draw_rate=self.get_content(tr_1, 11),
                max_new_lose_rate=self.get_content(tr_1, 12),
                max_new_win_kelly=self.get_content(tr_1, 13),
                max_new_draw_kelly=self.get_content(tr_1, 14),
                max_new_lose_kelly=self.get_content(tr_1, 15),
                max_pay_rate=self.get_content(tr_1, 16),

                min_init_win_odd=self.get_content(tr_2, 3),
                min_init_draw_odd=self.get_content(tr_2, 4),
                min_init_lose_odd=self.get_content(tr_2, 5),
                min_new_win_odd=tr_2.xpath('(.//td)[6]/a/text()').extract_first(),
                min_new_draw_odd=self.get_content(tr_2, 7),
                min_new_lose_odd=self.get_content(tr_2, 8),
                min_new_win_rate=self.get_content(tr_2, 10),
                min_new_draw_rate=self.get_content(tr_2, 11),
                min_new_lose_rate=self.get_content(tr_2, 12),
                min_new_win_kelly=self.get_content(tr_2, 13),
                min_new_draw_kelly=self.get_content(tr_2, 14),
                min_new_lose_kelly=self.get_content(tr_2, 15),
                min_pay_rate=self.get_content(tr_2, 16),

                dispersion_win = dispersions[0],
                dispersion_draw = dispersions[1],
                dispersion_lose = dispersions[2],

                std_win = stds[0],
                std_draw = stds[1],
                std_lose = stds[2],
            )

            # tr = trs[0]
            # yield self.get_item(tr, 'avg', response.meta['matchid'])

            # tr = trs[1]
            # yield self.get_item(tr, 'max', response.meta['matchid'])

            # tr = trs[2]
            # yield self.get_item(tr, 'min', response.meta['matchid'])
