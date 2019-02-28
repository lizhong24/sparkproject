# 创建sparkproject数据库
CREATE DATABASE `sparkproject`;

# 使用sparkproject数据库
USE `sparkproject`;

# 创建test_user表
CREATE TABLE `test_user` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(10) DEFAULT NULL,
  `age` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=5 DEFAULT CHARSET=utf8

# 创建task表
CREATE TABLE `task` (
  `task_id` int(11) NOT NULL AUTO_INCREMENT,
  `task_name` varchar(255) DEFAULT NULL,
  `create_time` varchar(255) DEFAULT NULL,
  `start_time` varchar(255) DEFAULT NULL,
  `finish_time` varchar(255) DEFAULT NULL,
  `task_type` varchar(255) DEFAULT NULL,
  `task_status` varchar(255) DEFAULT NULL,
  `task_param` text,
  PRIMARY KEY (`task_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8

# {"startAge":["10"],"endAge":["50"],"startDate":["2019-02-27"],"endDate":["2019-02-27"]}
UPDATE `sparkproject`.`task` SET `task_param` = '{\"startAge\":[\"10\"],\"endAge\":[\"50\"],\"startDate\":[\"2019-02-27\"],\"endDate\":[\"2019-02-27\"]}' WHERE `task_id` = '2';

# 创建session_aggr_stat表
CREATE TABLE `session_aggr_stat` (
  `task_id` int(11) DEFAULT NULL,
  `session_count` int(11) DEFAULT NULL,
  `1s_3s` double DEFAULT NULL,
  `4s_6s` double DEFAULT NULL,
  `7s_9s` double DEFAULT NULL,
  `10s_30s` double DEFAULT NULL,
  `30s_60s` double DEFAULT NULL,
  `1m_3m` double DEFAULT NULL,
  `3m_10m` double DEFAULT NULL,
  `10m_30m` double DEFAULT NULL,
  `30m` double DEFAULT NULL,
  `1_3` double DEFAULT NULL,
  `4_6` double DEFAULT NULL,
  `7_9` double DEFAULT NULL,
  `10_30` double DEFAULT NULL,
  `30_60` double DEFAULT NULL,
  `60` double DEFAULT NULL,
  KEY `idx_task_id` (`task_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8

# 创建session_random_extract表
CREATE TABLE `session_random_extract` (
  `task_id` int(11) DEFAULT NULL,
  `session_id` varchar(255) DEFAULT NULL,
  `start_time` varchar(50) DEFAULT NULL,
  `search_keywords` varchar(255) DEFAULT NULL,
  `click_category_ids` varchar(255) DEFAULT NULL,
  KEY `idx_task_id` (`task_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8

# 创建session_detail表
CREATE TABLE `session_detail` (
  `task_id` int(11) DEFAULT NULL,
  `user_id` int(11) DEFAULT NULL,
  `session_id` varchar(255) DEFAULT NULL,
  `page_id` int(11) DEFAULT NULL,
  `action_time` varchar(255) DEFAULT NULL,
  `search_keyword` varchar(255) DEFAULT NULL,
  `click_category_id` int(11) DEFAULT NULL,
  `click_product_id` int(11) DEFAULT NULL,
  `order_category_ids` varchar(255) DEFAULT NULL,
  `order_product_ids` varchar(255) DEFAULT NULL,
  `pay_category_ids` varchar(255) DEFAULT NULL,
  `pay_product_ids` varchar(255) DEFAULT NULL,
  KEY `idx_task_id` (`task_id`),
  KEY `idx_session_id` (`session_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8

# 创建top10_category表
CREATE TABLE `top10_category` (
  `task_id` int(11) DEFAULT NULL,
  `category_id` int(11) DEFAULT NULL,
  `click_count` int(11) DEFAULT NULL,
  `order_count` int(11) DEFAULT NULL,
  `pay_count` int(11) DEFAULT NULL,
  KEY `idx_task_id` (`task_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8

# 创建top10_session表
CREATE TABLE `top10_session` (
  `task_id` int(11) DEFAULT NULL,
  `category_id` int(11) DEFAULT NULL,
  `session_id` varchar(255) DEFAULT NULL,
  `click_count` int(11) DEFAULT NULL,
  KEY `idx_task_id` (`task_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8

