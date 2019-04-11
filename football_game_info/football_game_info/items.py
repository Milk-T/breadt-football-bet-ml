# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class FSpiderBriefInfo(scrapy.Item):
    matchid = scrapy.Field()
    status = scrapy.Field()
    game = scrapy.Field()
    turn = scrapy.Field()
    home_team = scrapy.Field()
    visit_team = scrapy.Field()
    gs = scrapy.Field()
    gd = scrapy.Field()
    gn = scrapy.Field()
    time = scrapy.Field()
    result = scrapy.Field()
    win_bet_return = scrapy.Field()
    draw_bet_return = scrapy.Field()
    lose_bet_return = scrapy.Field()


class FSpiderPredictInfo(scrapy.Item):
    fid = scrapy.Field()
    status = scrapy.Field()
    game = scrapy.Field()
    turn = scrapy.Field()
    home_team = scrapy.Field()
    visit_team = scrapy.Field()
    offset = scrapy.Field()
    time = scrapy.Field()


class FSpiderReferInfo(scrapy.Item):
    """docstring for FSpiderReferInfo"""

    fid = scrapy.Field()
    pos = scrapy.Field()
    name = scrapy.Field()
    date = scrapy.Field()
    host_team = scrapy.Field()
    visit_team = scrapy.Field()
    gs = scrapy.Field()
    gd = scrapy.Field()
    result = scrapy.Field()
    gn = scrapy.Field()
