CREATE TABLE `gp_info` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT COMMENT '主键ID',
  `market_time` varchar(10) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL COMMENT '时间',
  `gp_code` varchar(20) DEFAULT NULL COMMENT '股票CODE',
  `gp_name` varchar(40) DEFAULT NULL COMMENT '股票名称',
  `latest_price` decimal(8,2) DEFAULT NULL COMMENT '最新价',
  `amplitude` bigint DEFAULT NULL COMMENT '振幅',
  `rise_fall_amount` decimal(8,2) DEFAULT NULL COMMENT '涨跌额',
  `rise_fall_rang` bigint DEFAULT NULL COMMENT '涨跌幅',
  `turnover_rate` decimal(10,2) DEFAULT NULL COMMENT '换手率',
  `bug_in` decimal(8,2) DEFAULT NULL COMMENT '买入',
  `sell_out` decimal(8,2) DEFAULT NULL COMMENT '卖出',
  `yesterday_close_price` decimal(10,2) DEFAULT NULL COMMENT '昨收',
  `today_open_price` decimal(10,2) DEFAULT NULL COMMENT '今开',
  `highest_price` decimal(10,2) DEFAULT NULL COMMENT '最高',
  `minimum_price` decimal(10,2) DEFAULT NULL COMMENT '最低',
  `turnover` decimal(15,2) DEFAULT NULL COMMENT '成交量',
  `transaction_volume` decimal(15,2) DEFAULT NULL COMMENT '成交额',
  `create_time` datetime DEFAULT NULL COMMENT '创建时间',
  PRIMARY KEY (`id`),
  KEY `index_market_time` (`market_time`),
  KEY `index_gp_code` (`gp_code`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;