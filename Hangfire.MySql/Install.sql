-- New installs use utf8mb4. Payload columns use utf8mb4_unicode_520_ci.
-- Identifier columns use utf8mb4_bin (see the COLLATE clauses below).
--
-- utf8mb4_general_ci compares every non-BMP character as equal, so distinct
-- emoji collapse on the Set unique key (AddToSet uses ON DUPLICATE KEY UPDATE).
-- utf8mb4_unicode_520_ci keeps different emoji apart, but it ignores some invisible
-- characters: values that differ only by U+200D (zero-width joiner) or a variation
-- selector such as U+FE0F compare equal, as do U+00DF (sharp s) and "ss".
-- No case-insensitive MySQL collation separates those pairs, so Key, Value,
-- Field, Resource, JobParameter.Name and Server.Id are utf8mb4_bin.
-- Comparisons on those columns are case-sensitive.
-- JobQueue.Queue stays utf8mb4_unicode_520_ci. Dequeue matches with
-- `Queue in @queues`, and Hangfire 1.6 does not normalize server queue names.
-- EnqueuedState only accepts [a-z0-9_]+, which unicode_520_ci does not fold.
-- MySQL NVARCHAR is VARCHAR CHARACTER SET utf8mb3, so string columns are VARCHAR
-- and inherit the table charset unless a column collation is set. Otherwise
-- emoji still fails on those columns.
-- Set.Score is DOUBLE (64-bit). MySQL FLOAT is 32-bit and drops timestamp precision.
-- The installer does not alter existing databases. See README for ALTER statements.

-- ----------------------------
-- Table structure for `Job`
-- ----------------------------
CREATE TABLE `[tablesPrefix]Job` (
  `Id` int(11) NOT NULL AUTO_INCREMENT,
  `StateId` int(11) DEFAULT NULL,
  `StateName` varchar(20) DEFAULT NULL,
  `InvocationData` longtext NOT NULL,
  `Arguments` longtext NOT NULL,
  `CreatedAt` datetime(6) NOT NULL,
  `ExpireAt` datetime(6) DEFAULT NULL,
  PRIMARY KEY (`Id`),
  KEY `IX_[tablesPrefix]Job_StateName` (`StateName`)
) ENGINE=InnoDB DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_520_ci;


-- ----------------------------
-- Table structure for `Counter`
-- ----------------------------
CREATE TABLE `[tablesPrefix]Counter` (
  `Id` int(11) NOT NULL AUTO_INCREMENT,
  `Key` varchar(100) COLLATE utf8mb4_bin NOT NULL,
  `Value` int(11) NOT NULL,
  `ExpireAt` datetime DEFAULT NULL,
  PRIMARY KEY (`Id`),
  KEY `IX_[tablesPrefix]Counter_Key` (`Key`)
) ENGINE=InnoDB DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_520_ci;


CREATE TABLE `[tablesPrefix]AggregatedCounter` (
	Id int(11) NOT NULL AUTO_INCREMENT,
  `Key` varchar(100) COLLATE utf8mb4_bin NOT NULL,
	`Value` int(11) NOT NULL,
	ExpireAt datetime DEFAULT NULL,
	PRIMARY KEY (`Id`),
	UNIQUE KEY `IX_[tablesPrefix]CounterAggregated_Key` (`Key`)
) ENGINE=InnoDB DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_520_ci;


-- ----------------------------
-- Table structure for `DistributedLock`
-- ----------------------------
CREATE TABLE `[tablesPrefix]DistributedLock` (
  `Resource` varchar(100) COLLATE utf8mb4_bin NOT NULL,
  `CreatedAt` datetime(6) NOT NULL
) ENGINE=InnoDB DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_520_ci;


-- ----------------------------
-- Table structure for `Hash`
-- ----------------------------
CREATE TABLE `[tablesPrefix]Hash` (
  `Id` int(11) NOT NULL AUTO_INCREMENT,
  `Key` varchar(100) COLLATE utf8mb4_bin NOT NULL,
  `Field` varchar(40) COLLATE utf8mb4_bin NOT NULL,
  `Value` longtext,
  `ExpireAt` datetime(6) DEFAULT NULL,
  PRIMARY KEY (`Id`),
  UNIQUE KEY `IX_[tablesPrefix]Hash_Key_Field` (`Key`,`Field`)
) ENGINE=InnoDB DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_520_ci;


-- ----------------------------
-- Table structure for `JobParameter`
-- ----------------------------
CREATE TABLE `[tablesPrefix]JobParameter` (
  `Id` int(11) NOT NULL AUTO_INCREMENT,
  `JobId` int(11) NOT NULL,
  `Name` varchar(40) COLLATE utf8mb4_bin NOT NULL,
  `Value` longtext,
  PRIMARY KEY (`Id`),
  CONSTRAINT `IX_[tablesPrefix]JobParameter_JobId_Name` UNIQUE (`JobId`,`Name`),
  KEY `FK_[tablesPrefix]JobParameter_Job` (`JobId`),
  CONSTRAINT `FK_[tablesPrefix]JobParameter_Job` FOREIGN KEY (`JobId`) REFERENCES `[tablesPrefix]Job` (`Id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_520_ci;

-- ----------------------------
-- Table structure for `JobQueue`
-- ----------------------------
CREATE TABLE `[tablesPrefix]JobQueue` (
  `Id` int(11) NOT NULL AUTO_INCREMENT,
  `JobId` int(11) NOT NULL,
  `FetchedAt` datetime(6) DEFAULT NULL,
  -- Case-insensitive: dequeue uses `Queue in @queues`, and server queue
  -- names are not normalized. Valid Hangfire queue names are [a-z0-9_]+.
  `Queue` varchar(50) NOT NULL,
  `FetchToken` varchar(36) DEFAULT NULL,
  PRIMARY KEY (`Id`),
  INDEX `IX_[tablesPrefix]JobQueue_QueueAndFetchedAt` (`Queue`,`FetchedAt`)
) ENGINE=InnoDB DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_520_ci;

-- ----------------------------
-- Table structure for `JobState`
-- ----------------------------
CREATE TABLE `[tablesPrefix]JobState` (
  `Id` int(11) NOT NULL AUTO_INCREMENT,
  `JobId` int(11) NOT NULL,
  `CreatedAt` datetime(6) NOT NULL,
  `Name` varchar(20) NOT NULL,
  `Reason` varchar(100) DEFAULT NULL,
  `Data` longtext,
  PRIMARY KEY (`Id`),
  KEY `FK_[tablesPrefix]JobState_Job` (`JobId`),
  CONSTRAINT `FK_[tablesPrefix]JobState_Job` FOREIGN KEY (`JobId`) REFERENCES `[tablesPrefix]Job` (`Id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_520_ci;

-- ----------------------------
-- Table structure for `Server`
-- ----------------------------
CREATE TABLE `[tablesPrefix]Server` (
  `Id` varchar(100) COLLATE utf8mb4_bin NOT NULL,
  `Data` longtext NOT NULL,
  `LastHeartbeat` datetime(6) DEFAULT NULL,
  PRIMARY KEY (`Id`)
) ENGINE=InnoDB DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_520_ci;


-- ----------------------------
-- Table structure for `Set`
-- ----------------------------
CREATE TABLE `[tablesPrefix]Set` (
  `Id` int(11) NOT NULL AUTO_INCREMENT,
  `Key` varchar(100) COLLATE utf8mb4_bin NOT NULL,
  `Value` varchar(256) COLLATE utf8mb4_bin NOT NULL,
  `Score` double NOT NULL,
  `ExpireAt` datetime DEFAULT NULL,
  PRIMARY KEY (`Id`),
  UNIQUE KEY `IX_[tablesPrefix]Set_Key_Value` (`Key`,`Value`)
) ENGINE=InnoDB DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_520_ci;

CREATE TABLE `[tablesPrefix]State`
(
	Id int(11) NOT NULL AUTO_INCREMENT,
	JobId int(11) NOT NULL,
  Name varchar(20) NOT NULL,
  Reason varchar(100) NULL,
	CreatedAt datetime(6) NOT NULL,
	Data longtext NULL,
	PRIMARY KEY (`Id`),
	KEY `FK_[tablesPrefix]HangFire_State_Job` (`JobId`),
	CONSTRAINT `FK_[tablesPrefix]HangFire_State_Job` FOREIGN KEY (`JobId`) REFERENCES `[tablesPrefix]Job` (`Id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_520_ci;

CREATE TABLE `[tablesPrefix]List`
(
	`Id` int(11) NOT NULL AUTO_INCREMENT,
  `Key` varchar(100) COLLATE utf8mb4_bin NOT NULL,
	`Value` longtext COLLATE utf8mb4_bin NULL,
	`ExpireAt` datetime(6) NULL,
	PRIMARY KEY (`Id`)
) ENGINE=InnoDB DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_520_ci;