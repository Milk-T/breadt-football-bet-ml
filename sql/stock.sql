CREATE TABLE `stock_info` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `code` varchar(10) NOT NULL DEFAULT '',
  `title` text NOT NULL,
  `link` varchar(255) DEFAULT NULL,
  `date` datetime NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=867724 DEFAULT CHARSET=utf8;