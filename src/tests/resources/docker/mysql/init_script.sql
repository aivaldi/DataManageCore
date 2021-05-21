CREATE TABLE `testdb`.`test1` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `code` INT NOT NULL,
  `val` DECIMAL(12) NULL,
  `quantity` INT NULL,
  `text1` VARCHAR(45) NULL,
  `text2` VARCHAR(45) NULL,
  `dateString` DATETIME NULL ,
  PRIMARY KEY (`Id`));

INSERT INTO `testdb`.`test1` (`code`, `val`, `quantity`, `text1`, `text2`, dateString) VALUES ('1', '10', '1', 'space     s     ss', null, '2019-01-20');
INSERT INTO `testdb`.`test1`  (`code`, `val`, `quantity`,dateString )  VALUES ('2', '30', '2','2019-02-20');
INSERT INTO `testdb`.`test1`  (`code`, `val`, `quantity`,dateString)  VALUES ('3', '100', '9', '2019-03-20');
INSERT INTO `testdb`.`test1`  (`code`, `val`, `quantity`,dateString)  VALUES ('1', '12', '2','2019-04-20');
INSERT INTO `testdb`.`test1`  (`code`, `val`, `quantity`,dateString)  VALUES ('2', '33', '3','2019-05-20');
INSERT INTO `testdb`.`test1`  (`code`, `val`, `quantity`,dateString)  VALUES ('3', '12', '8','2019-06-20');

CREATE TABLE `testdb`.`test2` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `code` INT NOT NULL,
  `val` DECIMAL(12) NULL,
  `quantity` INT NULL,
  `text1` VARCHAR(45) NULL,
  `text2` VARCHAR(45) NULL,
  `dateString` DATETIME NULL ,
  PRIMARY KEY (`Id`));

INSERT INTO `testdb`.`test2` (`code`, `val`, `quantity`, `text1`, `text2`, dateString) VALUES ('1', '10', '1', 'space     s     ss', null, '2019-01-20');
INSERT INTO `testdb`.`test2`  (`code`, `val`, `quantity`,dateString )  VALUES ('2', '30', '2','2019-02-20');
INSERT INTO `testdb`.`test2`  (`code`, `val`, `quantity`,dateString)  VALUES ('3', '100', '9', '2019-03-20');
INSERT INTO `testdb`.`test2`  (`code`, `val`, `quantity`,dateString)  VALUES ('1', '12', '2','2019-04-20');
INSERT INTO `testdb`.`test2`  (`code`, `val`, `quantity`,dateString)  VALUES ('2', '33', '3','2019-05-20');
INSERT INTO `testdb`.`test2`  (`code`, `val`, `quantity`,dateString)  VALUES ('3', '12', '8','2019-06-20');




CREATE TABLE `testdb`.`test3` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `code` varchar(50) NOT NULL,
  `jan` INT NULL,
  `feb` INT NULL,
  `march` INT NULL,
  `april` INT NULL,
  PRIMARY KEY (`Id`));

INSERT INTO `testdb`.`test3`  VALUES (null,'anc', 10,5, 3, 2);
INSERT INTO `testdb`.`test3`  VALUES (null,'anc', 4,3, 2, 1);