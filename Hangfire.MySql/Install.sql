-- ----------------------------
-- Table structure for `Job`
-- ----------------------------
CREATE TABLE `Job` (
  `Id` int(11) NOT NULL AUTO_INCREMENT,
  `StateId` int(11) DEFAULT NULL,
  `StateName` nvarchar(20) DEFAULT NULL,
  `InvocationData` longtext NOT NULL,
  `Arguments` longtext NOT NULL,
  `CreatedAt` datetime(6) NOT NULL,
  `ExpireAt` datetime(6) DEFAULT NULL,
  PRIMARY KEY (`Id`),
  KEY `IX_Job_StateName` (`StateName`)
) ENGINE=InnoDB DEFAULT CHARACTER SET utf8 COLLATE utf8_general_ci;


-- ----------------------------
-- Table structure for `Counter`
-- ----------------------------
CREATE TABLE `Counter` (
  `Id` int(11) NOT NULL AUTO_INCREMENT,
  `Key` nvarchar(100) NOT NULL,
  `Value` int(11) NOT NULL,
  `ExpireAt` datetime DEFAULT NULL,
  PRIMARY KEY (`Id`),
  KEY `IX_Counter_Key` (`Key`)
) ENGINE=InnoDB DEFAULT CHARACTER SET utf8 COLLATE utf8_general_ci;


CREATE TABLE `AggregatedCounter` (
	Id int(11) NOT NULL AUTO_INCREMENT,
  `Key` nvarchar(100) NOT NULL,
	`Value` int(11) NOT NULL,
	ExpireAt datetime DEFAULT NULL,
	PRIMARY KEY (`Id`),
	UNIQUE KEY `IX_CounterAggregated_Key` (`Key`)
) ENGINE=InnoDB DEFAULT CHARACTER SET utf8 COLLATE utf8_general_ci;


-- ----------------------------
-- Table structure for `DistributedLock`
-- ----------------------------
CREATE TABLE `DistributedLock` (
  `Resource` nvarchar(100) NOT NULL,
  `CreatedAt` datetime(6) NOT NULL
) ENGINE=InnoDB DEFAULT CHARACTER SET utf8 COLLATE utf8_general_ci;


-- ----------------------------
-- Table structure for `Hash`
-- ----------------------------
CREATE TABLE `Hash` (
  `Id` int(11) NOT NULL AUTO_INCREMENT,
  `Key` nvarchar(100) NOT NULL,
  `Field` nvarchar(40) NOT NULL,
  `Value` longtext,
  `ExpireAt` datetime(6) DEFAULT NULL,
  PRIMARY KEY (`Id`),
  UNIQUE KEY `IX_Hash_Key_Field` (`Key`,`Field`)
) ENGINE=InnoDB DEFAULT CHARACTER SET utf8 COLLATE utf8_general_ci;


-- ----------------------------
-- Table structure for `JobParameter`
-- ----------------------------
CREATE TABLE `JobParameter` (
  `Id` int(11) NOT NULL AUTO_INCREMENT,
  `JobId` int(11) NOT NULL,
  `Name` nvarchar(40) NOT NULL,
  `Value` longtext,
  PRIMARY KEY (`Id`),
  CONSTRAINT `IX_JobParameter_JobId_Name` UNIQUE (`JobId`,`Name`),
  KEY `FK_JobParameter_Job` (`JobId`),
  CONSTRAINT `FK_JobParameter_Job` FOREIGN KEY (`JobId`) REFERENCES `Job` (`Id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARACTER SET utf8 COLLATE utf8_general_ci;

-- ----------------------------
-- Table structure for `JobQueue`
-- ----------------------------
CREATE TABLE `JobQueue` (
  `Id` int(11) NOT NULL AUTO_INCREMENT,
  `JobId` int(11) NOT NULL,
  `FetchedAt` datetime(6) DEFAULT NULL,
  `Queue` nvarchar(50) NOT NULL,
  `FetchToken` nvarchar(36) DEFAULT NULL,
  PRIMARY KEY (`Id`),
  INDEX `IX_JobQueue_QueueAndFetchedAt` (`Queue`,`FetchedAt`)
) ENGINE=InnoDB DEFAULT CHARACTER SET utf8 COLLATE utf8_general_ci;

-- ----------------------------
-- Table structure for `JobState`
-- ----------------------------
CREATE TABLE `JobState` (
  `Id` int(11) NOT NULL AUTO_INCREMENT,
  `JobId` int(11) NOT NULL,
  `CreatedAt` datetime(6) NOT NULL,
  `Name` nvarchar(20) NOT NULL,
  `Reason` nvarchar(100) DEFAULT NULL,
  `Data` longtext,
  PRIMARY KEY (`Id`),
  KEY `FK_JobState_Job` (`JobId`),
  CONSTRAINT `FK_JobState_Job` FOREIGN KEY (`JobId`) REFERENCES `Job` (`Id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARACTER SET utf8 COLLATE utf8_general_ci;

-- ----------------------------
-- Table structure for `Server`
-- ----------------------------
CREATE TABLE `Server` (
  `Id` nvarchar(100) NOT NULL,
  `Data` longtext NOT NULL,
  `LastHeartbeat` datetime(6) DEFAULT NULL,
  PRIMARY KEY (`Id`)
) ENGINE=InnoDB DEFAULT CHARACTER SET utf8 COLLATE utf8_general_ci;


-- ----------------------------
-- Table structure for `Set`
-- ----------------------------
CREATE TABLE `Set` (
  `Id` int(11) NOT NULL AUTO_INCREMENT,
  `Key` nvarchar(100) NOT NULL,
  `Value` nvarchar(256) NOT NULL,
  `Score` float NOT NULL,
  `ExpireAt` datetime DEFAULT NULL,
  PRIMARY KEY (`Id`),
  UNIQUE KEY `IX_Set_Key_Value` (`Key`,`Value`)
) ENGINE=InnoDB DEFAULT CHARACTER SET utf8 COLLATE utf8_general_ci;

CREATE TABLE `State`
(
	Id int(11) NOT NULL AUTO_INCREMENT,
	JobId int(11) NOT NULL,
  Name nvarchar(20) NOT NULL,
  Reason nvarchar(100) NULL,
	CreatedAt datetime(6) NOT NULL,
	Data longtext NULL,
	PRIMARY KEY (`Id`),
	KEY `FK_HangFire_State_Job` (`JobId`),
	CONSTRAINT `FK_HangFire_State_Job` FOREIGN KEY (`JobId`) REFERENCES `Job` (`Id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARACTER SET utf8 COLLATE utf8_general_ci;

CREATE TABLE `List`
(
	`Id` int(11) NOT NULL AUTO_INCREMENT,
  `Key` nvarchar(100) NOT NULL,
	`Value` longtext NULL,
	`ExpireAt` datetime(6) NULL,
	PRIMARY KEY (`Id`)
) ENGINE=InnoDB DEFAULT CHARACTER SET utf8 COLLATE utf8_general_ci;