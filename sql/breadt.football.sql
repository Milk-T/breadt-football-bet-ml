CREATE TABLE `breadt_football_game_list` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `fid` bigint(20) NOT NULL,
  `status` varchar(20) NOT NULL DEFAULT '',
  `game` varchar(100) DEFAULT '',
  `turn` varchar(100) DEFAULT '',
  `home_team` varchar(255) NOT NULL DEFAULT '',
  `visit_team` varchar(255) NOT NULL DEFAULT '',
  `gs` int(4) NOT NULL COMMENT '主队进球数',
  `gd` int(4) NOT NULL COMMENT '客队进球数',
  `gn` int(4) NOT NULL COMMENT '总进球数',
  `offset` varchar(20) DEFAULT NULL,
  `time` datetime NOT NULL,
  `result` int(4) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=79145 DEFAULT CHARSET=utf8;

CREATE TABLE `breadt_football_predict_game` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `fid` bigint(20) NOT NULL,
  `status` varchar(20) NOT NULL DEFAULT '',
  `game` varchar(100) DEFAULT '',
  `turn` varchar(100) DEFAULT '',
  `home_team` varchar(255) NOT NULL DEFAULT '',
  `visit_team` varchar(255) NOT NULL DEFAULT '',
  `offset` varchar(20) DEFAULT NULL,
  `time` datetime NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=3 DEFAULT CHARSET=utf8;

CREATE TABLE `breadt_football_refer_games` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `fid` int(20) NOT NULL,
  `pos` varchar(20) NOT NULL DEFAULT '',
  `gd` int(4) NOT NULL,
  `gs` int(4) NOT NULL,
  `gn` int(4) NOT NULL,
  `result` int(4) NOT NULL,
  `home_team` varchar(255) NOT NULL DEFAULT '',
  `visit_team` varchar(255) NOT NULL DEFAULT '',
  `name` varchar(200) DEFAULT NULL,
  `date` datetime DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=3521117 DEFAULT CHARSET=utf8;