-- ----------------------------
-- Table structure for `Job`
-- ----------------------------
CREATE TABLE `[tablesPrefix]Job` (
  `Id` int(11) NOT NULL AUTO_INCREMENT,
  `StateId` int(11) DEFAULT NULL,
  `StateName` nvarchar(20) DEFAULT NULL,
  `InvocationData` longtext NOT NULL,
  `Arguments` longtext NOT NULL,
  `CreatedAt` datetime(6) NOT NULL,
  `ExpireAt` datetime(6) DEFAULT NULL,
  PRIMARY KEY (`Id`),
  KEY `IX_[tablesPrefix]Job_StateName` (`StateName`)
) ENGINE=InnoDB DEFAULT CHARACTER SET utf8 COLLATE utf8_general_ci;


-- ----------------------------
-- Table structure for `Counter`
-- ----------------------------
CREATE TABLE `[tablesPrefix]Counter` (
  `Id` int(11) NOT NULL AUTO_INCREMENT,
  `Key` nvarchar(100) NOT NULL,
  `Value` int(11) NOT NULL,
  `ExpireAt` datetime DEFAULT NULL,
  PRIMARY KEY (`Id`),
  KEY `IX_[tablesPrefix]Counter_Key` (`Key`)
) ENGINE=InnoDB DEFAULT CHARACTER SET utf8 COLLATE utf8_general_ci;


CREATE TABLE `[tablesPrefix]AggregatedCounter` (
	Id int(11) NOT NULL AUTO_INCREMENT,
  `Key` nvarchar(100) NOT NULL,
	`Value` int(11) NOT NULL,
	ExpireAt datetime DEFAULT NULL,
	PRIMARY KEY (`Id`),
	UNIQUE KEY `IX_[tablesPrefix]CounterAggregated_Key` (`Key`)
) ENGINE=InnoDB DEFAULT CHARACTER SET utf8 COLLATE utf8_general_ci;


-- ----------------------------
-- Table structure for `DistributedLock`
-- ----------------------------
CREATE TABLE `[tablesPrefix]DistributedLock` (
  `Resource` nvarchar(100) NOT NULL,
  `CreatedAt` datetime(6) NOT NULL
) ENGINE=InnoDB DEFAULT CHARACTER SET utf8 COLLATE utf8_general_ci;


-- ----------------------------
-- Table structure for `Hash`
-- ----------------------------
CREATE TABLE `[tablesPrefix]Hash` (
  `Id` int(11) NOT NULL AUTO_INCREMENT,
  `Key` nvarchar(100) NOT NULL,
  `Field` nvarchar(40) NOT NULL,
  `Value` longtext,
  `ExpireAt` datetime(6) DEFAULT NULL,
  PRIMARY KEY (`Id`),
  UNIQUE KEY `IX_[tablesPrefix]Hash_Key_Field` (`Key`,`Field`)
) ENGINE=InnoDB DEFAULT CHARACTER SET utf8 COLLATE utf8_general_ci;


-- ----------------------------
-- Table structure for `JobParameter`
-- ----------------------------
CREATE TABLE `[tablesPrefix]JobParameter` (
  `Id` int(11) NOT NULL AUTO_INCREMENT,
  `JobId` int(11) NOT NULL,
  `Name` nvarchar(40) NOT NULL,
  `Value` longtext,
  PRIMARY KEY (`Id`),
  CONSTRAINT `IX_[tablesPrefix]JobParameter_JobId_Name` UNIQUE (`JobId`,`Name`),
  KEY `FK_[tablesPrefix]JobParameter_Job` (`JobId`),
  CONSTRAINT `FK_[tablesPrefix]JobParameter_Job` FOREIGN KEY (`JobId`) REFERENCES `[tablesPrefix]Job` (`Id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARACTER SET utf8 COLLATE utf8_general_ci;

-- ----------------------------
-- Table structure for `JobQueue`
-- ----------------------------
CREATE TABLE `[tablesPrefix]JobQueue` (
  `Id` int(11) NOT NULL AUTO_INCREMENT,
  `JobId` int(11) NOT NULL,
  `FetchedAt` datetime(6) DEFAULT NULL,
  `Queue` nvarchar(50) NOT NULL,
  `FetchToken` nvarchar(36) DEFAULT NULL,
  PRIMARY KEY (`Id`),
  INDEX `IX_[tablesPrefix]JobQueue_QueueAndFetchedAt` (`Queue`,`FetchedAt`)
) ENGINE=InnoDB DEFAULT CHARACTER SET utf8 COLLATE utf8_general_ci;

-- ----------------------------
-- Table structure for `JobState`
-- ----------------------------
CREATE TABLE `[tablesPrefix]JobState` (
  `Id` int(11) NOT NULL AUTO_INCREMENT,
  `JobId` int(11) NOT NULL,
  `CreatedAt` datetime(6) NOT NULL,
  `Name` nvarchar(20) NOT NULL,
  `Reason` nvarchar(100) DEFAULT NULL,
  `Data` longtext,
  PRIMARY KEY (`Id`),
  KEY `FK_[tablesPrefix]JobState_Job` (`JobId`),
  CONSTRAINT `FK_[tablesPrefix]JobState_Job` FOREIGN KEY (`JobId`) REFERENCES `[tablesPrefix]Job` (`Id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARACTER SET utf8 COLLATE utf8_general_ci;

-- ----------------------------
-- Table structure for `Server`
-- ----------------------------
CREATE TABLE `[tablesPrefix]Server` (
  `Id` nvarchar(100) NOT NULL,
  `Data` longtext NOT NULL,
  `LastHeartbeat` datetime(6) DEFAULT NULL,
  PRIMARY KEY (`Id`)
) ENGINE=InnoDB DEFAULT CHARACTER SET utf8 COLLATE utf8_general_ci;


-- ----------------------------
-- Table structure for `Set`
-- ----------------------------
CREATE TABLE `[tablesPrefix]Set` (
  `Id` int(11) NOT NULL AUTO_INCREMENT,
  `Key` nvarchar(100) NOT NULL,
  `Value` nvarchar(256) NOT NULL,
  `Score` float NOT NULL,
  `ExpireAt` datetime DEFAULT NULL,
  PRIMARY KEY (`Id`),
  UNIQUE KEY `IX_[tablesPrefix]Set_Key_Value` (`Key`,`Value`)
) ENGINE=InnoDB DEFAULT CHARACTER SET utf8 COLLATE utf8_general_ci;

CREATE TABLE `[tablesPrefix]State`
(
	Id int(11) NOT NULL AUTO_INCREMENT,
	JobId int(11) NOT NULL,
  Name nvarchar(20) NOT NULL,
  Reason nvarchar(100) NULL,
	CreatedAt datetime(6) NOT NULL,
	Data longtext NULL,
	PRIMARY KEY (`Id`),
	KEY `FK_[tablesPrefix]HangFire_State_Job` (`JobId`),
	CONSTRAINT `FK_[tablesPrefix]HangFire_State_Job` FOREIGN KEY (`JobId`) REFERENCES `[tablesPrefix]Job` (`Id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARACTER SET utf8 COLLATE utf8_general_ci;

CREATE TABLE `[tablesPrefix]List`
(
	`Id` int(11) NOT NULL AUTO_INCREMENT,
  `Key` nvarchar(100) NOT NULL,
	`Value` longtext NULL,
	`ExpireAt` datetime(6) NULL,
	PRIMARY KEY (`Id`)
) ENGINE=InnoDB DEFAULT CHARACTER SET utf8 COLLATE utf8_general_ci;