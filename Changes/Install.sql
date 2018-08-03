-- ----------------------------
-- Table structure for `HangfireJob`
-- ----------------------------
CREATE TABLE `HangfireJob` (
  `Id` int(11) NOT NULL AUTO_INCREMENT,
  `StateId` int(11) DEFAULT NULL,
  `StateName` varchar(20) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
  `InvocationData` longtext CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL,
  `Arguments` longtext CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL,
  `CreatedAt` datetime NOT NULL,
  `ExpireAt` datetime DEFAULT NULL,
  PRIMARY KEY (`Id`),
  KEY `IX_HangfireJob_StateName` (`StateName`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;


-- ----------------------------
-- Table structure for `HangfireCounter`
-- ----------------------------
CREATE TABLE `HangfireCounter` (
  `Id` int(11) NOT NULL AUTO_INCREMENT,
  `Key` varchar(100) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL,
  `Value` int(11) NOT NULL,
  `ExpireAt` datetime DEFAULT NULL,
  PRIMARY KEY (`Id`),
  KEY `IX_HangfireCounter_Key` (`Key`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;


CREATE TABLE `HangfireAggregatedCounter` (
	Id int(11) NOT NULL AUTO_INCREMENT,
	`Key` varchar(100) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL,
	`Value` int(11) NOT NULL,
	ExpireAt datetime DEFAULT NULL,
	PRIMARY KEY (`Id`),
	UNIQUE KEY `IX_HangfireCounterAggregated_Key` (`Key`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;


-- ----------------------------
-- Table structure for `HangfireDistributedLock`
-- ----------------------------
CREATE TABLE `HangfireDistributedLock` (
  `Resource` varchar(100) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL,
  `CreatedAt` datetime NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;


-- ----------------------------
-- Table structure for `HangfireHash`
-- ----------------------------
CREATE TABLE `HangfireHash` (
  `Id` int(11) NOT NULL AUTO_INCREMENT,
  `Key` varchar(100) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL,
  `Field` varchar(40) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL,
  `Value` longtext CHARACTER SET utf8 COLLATE utf8_unicode_ci,
  `ExpireAt` datetime DEFAULT NULL,
  PRIMARY KEY (`Id`),
  UNIQUE KEY `IX_HangfireHash_Key_Field` (`Key`,`Field`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;


-- ----------------------------
-- Table structure for `HangfireJobParameter`
-- ----------------------------
CREATE TABLE `HangfireJobParameter` (
  `Id` int(11) NOT NULL AUTO_INCREMENT,
  `JobId` int(11) NOT NULL,
  `Name` varchar(40) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL,
  `Value` longtext CHARACTER SET utf8 COLLATE utf8_unicode_ci ,

  PRIMARY KEY (`Id`),
  CONSTRAINT `IX_HangfireJobParameter_JobId_Name` UNIQUE (`JobId`,`Name`),
  KEY `FK_HangfireJobParameter_Job` (`JobId`),
  CONSTRAINT `FK_HangfireJobParameter_Job` FOREIGN KEY (`JobId`) REFERENCES `HangfireJob` (`Id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

-- ----------------------------
-- Table structure for `HangfireJobQueue`
-- ----------------------------
CREATE TABLE `HangfireJobQueue` (
  `Id` int(11) NOT NULL AUTO_INCREMENT,
  `JobId` int(11) NOT NULL,
  `Queue` varchar(50) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL,
  `FetchedAt` datetime DEFAULT NULL,
  `FetchToken` varchar(36) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
  
  PRIMARY KEY (`Id`),
  INDEX `IX_HangfireJobQueue_QueueAndFetchedAt` (`Queue`,`FetchedAt`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

-- ----------------------------
-- Table structure for `HangfireJobState`
-- ----------------------------
CREATE TABLE `HangfireJobState` (
  `Id` int(11) NOT NULL AUTO_INCREMENT,
  `JobId` int(11) NOT NULL,
  `Name` varchar(20) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL,
  `Reason` varchar(100) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
  `CreatedAt` datetime NOT NULL,
  `Data` longtext CHARACTER SET utf8 COLLATE utf8_unicode_ci,
  PRIMARY KEY (`Id`),
  KEY `FK_HangfireJobState_Job` (`JobId`),
  CONSTRAINT `FK_JobState_Job` FOREIGN KEY (`JobId`) REFERENCES `HangfireJob` (`Id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

-- ----------------------------
-- Table structure for `HangfireServer`
-- ----------------------------
CREATE TABLE `HangfireServer` (
  `Id` varchar(100) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL,
  `Data` longtext CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL,
  `LastHeartbeat` datetime DEFAULT NULL,
  PRIMARY KEY (`Id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;


-- ----------------------------
-- Table structure for `HangfireSet`
-- ----------------------------
CREATE TABLE `HangfireSet` (
  `Id` int(11) NOT NULL AUTO_INCREMENT,
  `Key` varchar(100) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL,
  `Value` varchar(256) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL,
  `Score` float NOT NULL,
  `ExpireAt` datetime DEFAULT NULL,
  PRIMARY KEY (`Id`),
  UNIQUE KEY `IX_HangfireSet_Key_Value` (`Key`,`Value`)
) ENGINE=InnoDB  CHARSET=latin1;



CREATE TABLE `HangfireState`
(
	Id int(11) NOT NULL AUTO_INCREMENT,
	JobId int(11) NOT NULL,
	Name varchar(20) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL,
	Reason varchar(100) CHARACTER SET utf8 COLLATE utf8_unicode_ci NULL,
	CreatedAt datetime NOT NULL,
	Data longtext CHARACTER SET utf8 COLLATE utf8_unicode_ci NULL,
	PRIMARY KEY (`Id`),
	KEY `FK_HangFire_State_Job` (`JobId`),
	CONSTRAINT `FK_HangFire_State_Job` FOREIGN KEY (`JobId`) REFERENCES `HangfireJob` (`Id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB  CHARSET=latin1;

CREATE TABLE `HangfireList`
(
	`Id` int(11) NOT NULL AUTO_INCREMENT,
	`Key` varchar(100) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL,
	`Value` longtext CHARACTER SET utf8 COLLATE utf8_unicode_ci NULL,
	`ExpireAt` datetime NULL,
	PRIMARY KEY (`Id`)
) ENGINE=InnoDB  CHARSET=latin1;