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


class FSpiderFeatureInfo(scrapy.Item):

    matchid = scrapy.Field()

    h_score = scrapy.Field()

    h_pervious_rank = scrapy.Field()
    h_current_rank = scrapy.Field()

    h_perf_win = scrapy.Field()
    h_perf_draw = scrapy.Field()
    h_perf_lose = scrapy.Field()
    h_host_win = scrapy.Field()
    h_host_draw = scrapy.Field()
    h_host_lose = scrapy.Field()
    h_battle_with_front_10_win = scrapy.Field()
    h_battle_with_front_10_draw = scrapy.Field()
    h_battle_with_front_10_lose = scrapy.Field()
    h_battle_with_end_10_win = scrapy.Field()
    h_battle_with_end_10_draw = scrapy.Field()
    h_battle_with_end_10_lose = scrapy.Field()

    h_perf_gs = scrapy.Field()
    h_perf_gd = scrapy.Field()
    h_perf_avg_gs = scrapy.Field()
    h_perf_avg_gd = scrapy.Field()
    h_host_gs = scrapy.Field()
    h_host_gd = scrapy.Field()
    h_host_avg_gs = scrapy.Field()
    h_host_avg_gd = scrapy.Field()
    h_r3_gs = scrapy.Field()
    h_r3_gd = scrapy.Field()
    h_r3_avg_gs = scrapy.Field()
    h_r3_avg_gd = scrapy.Field()

    h_perf_bet_high = scrapy.Field()
    h_perf_bet_low = scrapy.Field()
    h_host_bet_high = scrapy.Field()
    h_host_bet_low = scrapy.Field()

    h_host_0_1_goal = scrapy.Field()
    h_host_2_3_goal = scrapy.Field()
    h_host_ab_4_goal = scrapy.Field()
    h_host_0_goal = scrapy.Field()
    h_host_1_goal = scrapy.Field()
    h_host_2_goal = scrapy.Field()
    h_host_3_goal = scrapy.Field()
    h_host_4_goal = scrapy.Field()
    h_host_5_goal = scrapy.Field()
    h_host_6_goal = scrapy.Field()
    h_host_7_goal = scrapy.Field()

    v_score = scrapy.Field()

    v_pervious_rank = scrapy.Field()
    v_current_rank = scrapy.Field()

    v_perf_win = scrapy.Field()
    v_perf_draw = scrapy.Field()
    v_perf_lose = scrapy.Field()
    v_host_win = scrapy.Field()
    v_host_draw = scrapy.Field()
    v_host_lose = scrapy.Field()
    v_battle_with_front_10_win = scrapy.Field()
    v_battle_with_front_10_draw = scrapy.Field()
    v_battle_with_front_10_lose = scrapy.Field()
    v_battle_with_end_10_win = scrapy.Field()
    v_battle_with_end_10_draw = scrapy.Field()
    v_battle_with_end_10_lose = scrapy.Field()

    v_perf_gs = scrapy.Field()
    v_perf_gd = scrapy.Field()
    v_perf_avg_gs = scrapy.Field()
    v_perf_avg_gd = scrapy.Field()
    v_host_gs = scrapy.Field()
    v_host_gd = scrapy.Field()
    v_host_avg_gs = scrapy.Field()
    v_host_avg_gd = scrapy.Field()
    v_r3_gs = scrapy.Field()
    v_r3_gd = scrapy.Field()
    v_r3_avg_gs = scrapy.Field()
    v_r3_avg_gd = scrapy.Field()

    v_perf_bet_high = scrapy.Field()
    v_perf_bet_low = scrapy.Field()
    v_host_bet_high = scrapy.Field()
    v_host_bet_low = scrapy.Field()

    v_host_0_1_goal = scrapy.Field()
    v_host_2_3_goal = scrapy.Field()
    v_host_ab_4_goal = scrapy.Field()
    v_host_0_goal = scrapy.Field()
    v_host_1_goal = scrapy.Field()
    v_host_2_goal = scrapy.Field()
    v_host_3_goal = scrapy.Field()
    v_host_4_goal = scrapy.Field()
    v_host_5_goal = scrapy.Field()
    v_host_6_goal = scrapy.Field()
    v_host_7_goal = scrapy.Field()
