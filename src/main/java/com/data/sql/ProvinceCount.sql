DROP TABLE IF EXISTS `data_count`;
CREATE TABLE `wordcount` (
  `name` varchar(255) DEFAULT NULL,
  `count` int(10) DEFAULT NULL,
  `index` int(10) NOT NULL AUTO_INCREMENT,
  PRIMARY KEY (`index`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
