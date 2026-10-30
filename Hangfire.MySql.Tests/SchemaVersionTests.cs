using System;
using System.Collections.Generic;
using Dapper;
using Hangfire.Logging;
using MySqlConnector;
using Xunit;

namespace Hangfire.MySql.Tests
{
    public class SchemaVersionTests
    {
        [Fact]
        public void Install_RecordsLatestSchemaVersion()
        {
            UsingDatabase(connection =>
            {
                MySqlObjectsInstaller.Install(connection);

                Assert.Equal(MySqlObjectsInstaller.LatestSchemaVersion, ReadSchemaVersion(connection, null));
            });
        }

        [Fact]
        public void Install_UsesTablesPrefix_ForSchemaMarker()
        {
            UsingDatabase(connection =>
            {
                MySqlObjectsInstaller.Install(connection, "A_");

                Assert.Equal(MySqlObjectsInstaller.LatestSchemaVersion, ReadSchemaVersion(connection, "A_"));
                Assert.Equal(0, CountTables(connection, "Job"));
                Assert.Equal(1, CountTables(connection, "A_Job"));
                Assert.Equal(1, CountTables(connection, "A_Schema"));
            });
        }

        [Fact]
        public void Install_DoesNotInspectUnprefixedTables_WhenPrefixIsSet()
        {
            UsingDatabase(connection =>
            {
                connection.Execute(
                    "CREATE TABLE `Job` (`Id` int NOT NULL PRIMARY KEY, `Name` varchar(20) NOT NULL) " +
                    "ENGINE=InnoDB DEFAULT CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci");

                string[] warnings;
                using (var logs = new WarningLog())
                {
                    MySqlObjectsInstaller.Install(connection, "Pfx");
                    warnings = logs.Warnings.ToArray();
                }

                Assert.Empty(warnings);
                Assert.Equal(MySqlObjectsInstaller.LatestSchemaVersion, ReadSchemaVersion(connection, "Pfx"));
                Assert.Equal("utf8mb3", ReadCharset(connection, "Job", "Name"));
            });
        }

        [Fact]
        public void Install_StampsVersion_WhenExistingDatabaseAlreadyMatches()
        {
            UsingDatabase(connection =>
            {
                MySqlObjectsInstaller.Install(connection);
                connection.Execute("DROP TABLE `Schema`");

                string[] warnings;
                using (var logs = new WarningLog())
                {
                    MySqlObjectsInstaller.Install(connection);
                    warnings = logs.Warnings.ToArray();
                }

                Assert.Empty(warnings);
                Assert.Equal(MySqlObjectsInstaller.LatestSchemaVersion, ReadSchemaVersion(connection, null));
                Assert.Equal("double", ReadColumnType(connection, "Set", "Score"));
            });
        }

        [Fact]
        public void Install_SecondCall_KeepsASingleSchemaRow()
        {
            UsingDatabase(connection =>
            {
                MySqlObjectsInstaller.Install(connection);
                MySqlObjectsInstaller.Install(connection);

                Assert.Equal(1, connection.ExecuteScalar<int>("select count(*) from `Schema`"));
                Assert.Equal(MySqlObjectsInstaller.LatestSchemaVersion, ReadSchemaVersion(connection, null));
            });
        }

        [Fact]
        public void Install_WarnsAndDoesNotStamp_WhenCharsetIsLegacy()
        {
            UsingDatabase(connection =>
            {
                MySqlObjectsInstaller.Install(connection);
                connection.Execute("DROP TABLE `Schema`");
                connection.Execute("ALTER TABLE `Job` CONVERT TO CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci");

                string[] warnings;
                using (var logs = new WarningLog())
                {
                    MySqlObjectsInstaller.Install(connection);
                    warnings = logs.Warnings.ToArray();
                }

                Assert.Equal(0, CountTables(connection, "Schema"));
                Assert.Equal("utf8mb3", ReadCharset(connection, "Job", "StateName"));
                Assert.Single(warnings);
                Assert.Contains("utf8/utf8mb3", warnings[0]);
                Assert.Contains("Job", warnings[0]);
                Assert.Contains("table prefix (none)", warnings[0]);
                Assert.Contains("recorded schema version missing", warnings[0]);
            });
        }

        [Fact]
        public void Install_WarnsAndDoesNotStamp_WhenScoreIsFloat()
        {
            UsingDatabase(connection =>
            {
                MySqlObjectsInstaller.Install(connection, "Hf");
                connection.Execute("DROP TABLE `HfSchema`");
                connection.Execute("ALTER TABLE `HfSet` MODIFY `Score` float NOT NULL");

                string[] warnings;
                using (var logs = new WarningLog())
                {
                    MySqlObjectsInstaller.Install(connection, "Hf");
                    warnings = logs.Warnings.ToArray();
                }

                Assert.Equal(0, CountTables(connection, "HfSchema"));
                Assert.Equal("float", ReadColumnType(connection, "HfSet", "Score"));
                Assert.Single(warnings);
                Assert.Contains("`HfSet`.`Score` is still float", warnings[0]);
                Assert.Contains("table prefix Hf", warnings[0]);
                Assert.DoesNotContain("utf8/utf8mb3", warnings[0]);
            });
        }

        [Fact]
        public void Install_WarnsForUtf8Mb3Alias_OnMinimalLegacyTables()
        {
            UsingDatabase(connection =>
            {
                connection.Execute(
                    "CREATE TABLE `Job` (`Id` int NOT NULL PRIMARY KEY, `StateName` varchar(20) NULL) " +
                    "ENGINE=InnoDB DEFAULT CHARACTER SET utf8 COLLATE utf8_general_ci");
                connection.Execute(
                    "CREATE TABLE `Set` (`Id` int NOT NULL PRIMARY KEY, `Score` float NOT NULL) " +
                    "ENGINE=InnoDB DEFAULT CHARACTER SET utf8 COLLATE utf8_general_ci");

                string[] warnings;
                using (var logs = new WarningLog())
                {
                    MySqlObjectsInstaller.Install(connection);
                    warnings = logs.Warnings.ToArray();
                }

                Assert.Equal(0, CountTables(connection, "Schema"));
                Assert.Equal("float", ReadColumnType(connection, "Set", "Score"));
                Assert.Single(warnings);
                Assert.Contains("utf8/utf8mb3", warnings[0]);
                Assert.Contains("`Set`.`Score` is still float", warnings[0]);
            });
        }

        [Fact]
        public void Install_DoesNotStampPartialUtf8Mb4Database()
        {
            UsingDatabase(connection =>
            {
                connection.Execute(
                    "CREATE TABLE `Job` (`Id` int NOT NULL PRIMARY KEY, `StateName` varchar(20) NULL) " +
                    "ENGINE=InnoDB DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_520_ci");

                string[] warnings;
                using (var logs = new WarningLog())
                {
                    MySqlObjectsInstaller.Install(connection);
                    warnings = logs.Warnings.ToArray();
                }

                Assert.Empty(warnings);
                Assert.Equal(0, CountTables(connection, "Schema"));
            });
        }

        [Fact]
        public void Ctor_SkipsInstallAndCheck_WhenPrepareSchemaIfNecessaryIsFalse()
        {
            UsingDatabase((connection, connectionString) =>
            {
                connection.Execute(
                    "CREATE TABLE `Job` (`Id` int NOT NULL PRIMARY KEY, `StateName` varchar(20) NULL) " +
                    "ENGINE=InnoDB DEFAULT CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci");
                connection.Execute(
                    "CREATE TABLE `Set` (`Id` int NOT NULL PRIMARY KEY, `Score` float NOT NULL) " +
                    "ENGINE=InnoDB DEFAULT CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci");

                string[] warnings;
                using (var logs = new WarningLog())
                {
                    var storage = new MySqlStorage(
                        connectionString,
                        new MySqlStorageOptions { PrepareSchemaIfNecessary = false });
                    Assert.NotNull(storage);
                    warnings = logs.Warnings.ToArray();
                }

                Assert.Empty(warnings);
                Assert.Equal(0, CountTables(connection, "Schema"));
                Assert.Equal("float", ReadColumnType(connection, "Set", "Score"));
                Assert.Equal("utf8mb3", ReadCharset(connection, "Job", "StateName"));
            });
        }

        [Fact]
        public void Ctor_PassesTablesPrefix_WhenPrepareSchemaIfNecessaryIsTrue()
        {
            UsingDatabase((connection, connectionString) =>
            {
                var storage = new MySqlStorage(
                    connectionString,
                    new MySqlStorageOptions
                    {
                        PrepareSchemaIfNecessary = true,
                        TablesPrefix = "Hangfire"
                    });
                Assert.NotNull(storage);

                Assert.Equal(MySqlObjectsInstaller.LatestSchemaVersion, ReadSchemaVersion(connection, "Hangfire"));
                Assert.Equal(0, CountTables(connection, "Job"));
                Assert.Equal("double", ReadColumnType(connection, "HangfireSet", "Score"));
            });
        }

        private static int? ReadSchemaVersion(MySqlConnection connection, string tablesPrefix)
        {
            var tableName = (tablesPrefix ?? string.Empty) + "Schema";
            if (CountTables(connection, tableName) == 0)
            {
                return null;
            }

            return connection.ExecuteScalar<int?>("select max(`Version`) from `" + tableName + "`");
        }

        private static int CountTables(MySqlConnection connection, string tableName)
        {
            return connection.ExecuteScalar<int>(
                "select count(*) from information_schema.tables " +
                "where table_schema = database() and table_name = @tableName",
                new { tableName });
        }

        private static string ReadCharset(MySqlConnection connection, string tableName, string columnName)
        {
            return connection.ExecuteScalar<string>(
                "select character_set_name from information_schema.columns " +
                "where table_schema = database() and table_name = @tableName and column_name = @columnName",
                new { tableName, columnName });
        }

        private static string ReadColumnType(MySqlConnection connection, string tableName, string columnName)
        {
            return connection.ExecuteScalar<string>(
                "select data_type from information_schema.columns " +
                "where table_schema = database() and table_name = @tableName and column_name = @columnName",
                new { tableName, columnName });
        }

        private static void UsingDatabase(Action<MySqlConnection> action)
        {
            UsingDatabase((connection, connectionString) => action(connection));
        }

        private static void UsingDatabase(Action<MySqlConnection, string> action)
        {
            var database = "hf_schema_" + Guid.NewGuid().ToString("N");
            var builder = new MySqlConnectionStringBuilder(ConnectionUtils.GetConnectionString())
            {
                Database = database
            };

            using (var master = new MySqlConnection(ConnectionUtils.GetMasterConnectionString()))
            {
                master.Open();
                master.Execute("CREATE DATABASE `" + database + "`");
            }

            try
            {
                using (var connection = new MySqlConnection(builder.ConnectionString))
                {
                    connection.Open();
                    action(connection, builder.ConnectionString);
                }
            }
            finally
            {
                using (var master = new MySqlConnection(ConnectionUtils.GetMasterConnectionString()))
                {
                    master.Open();
                    master.Execute("DROP DATABASE IF EXISTS `" + database + "`");
                }
            }
        }

        private sealed class WarningLog : IDisposable
        {
            private readonly CollectingLogProvider _provider = new CollectingLogProvider();

            public WarningLog()
            {
                LogProvider.SetCurrentLogProvider(_provider);
            }

            public List<string> Warnings
            {
                get { return _provider.Warnings; }
            }

            public void Dispose()
            {
            }

            private sealed class CollectingLogProvider : ILogProvider
            {
                public readonly List<string> Warnings = new List<string>();

                public ILog GetLogger(string name)
                {
                    return new CollectingLog(Warnings);
                }
            }

            private sealed class CollectingLog : ILog
            {
                private readonly List<string> _warnings;

                public CollectingLog(List<string> warnings)
                {
                    _warnings = warnings;
                }

                public bool Log(LogLevel logLevel, Func<string> messageFunc, Exception exception)
                {
                    if (messageFunc == null)
                    {
                        return logLevel == LogLevel.Warn || logLevel == LogLevel.Info || logLevel == LogLevel.Error;
                    }

                    if (logLevel == LogLevel.Warn)
                    {
                        _warnings.Add(messageFunc());
                    }

                    return true;
                }
            }
        }
    }
}
