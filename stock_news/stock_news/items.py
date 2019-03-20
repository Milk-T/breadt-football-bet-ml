# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class SSpiderBriefInfo(scrapy.Item):
    code = scrapy.Field()
    name = scrapy.Field()
    full_name = scrapy.Field()

class SSpiderInfoBriefInfo(scrapy.Item):
	code = scrapy.Field()
	link = scrapy.Field()
	title = scrapy.Field()
	date = scrapy.Field()
	signal = scrapy.Field()