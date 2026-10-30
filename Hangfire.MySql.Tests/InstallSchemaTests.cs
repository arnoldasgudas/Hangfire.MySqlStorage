using System;
using System.Collections.Generic;
using System.Linq;
using Dapper;
using Xunit;

namespace Hangfire.MySql.Tests
{
    public class InstallSchemaTests : IClassFixture<TestDatabaseFixture>
    {
        private static readonly HashSet<string> BinaryColumns = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
        {
            "Counter.Key",
            "AggregatedCounter.Key",
            "DistributedLock.Resource",
            "Hash.Key",
            "Hash.Field",
            "JobParameter.Name",
            "Server.Id",
            "Set.Key",
            "Set.Value",
            "List.Key",
            "List.Value"
        };

        [Fact, CleanDatabase]
        public void Install_UsesUtf8mb4AndBinaryCollationForIdentifierColumns()
        {
            using (var connection = ConnectionUtils.CreateConnection())
            {
                var columns = connection.Query(
                    "select table_name as TableName, column_name as ColumnName, " +
                    "character_set_name as CharsetName, collation_name as CollationName " +
                    "from information_schema.columns " +
                    "where table_schema = database() and character_set_name is not null").ToList();

                var mismatches = new List<string>();
                var found = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

                foreach (var column in columns)
                {
                    var name = (string)column.TableName + "." + (string)column.ColumnName;
                    found.Add(name);

                    if ((string)column.CharsetName != "utf8mb4")
                    {
                        mismatches.Add(name + " charset " + column.CharsetName);
                        continue;
                    }

                    var expected = BinaryColumns.Contains(name)
                        ? "utf8mb4_bin"
                        : "utf8mb4_unicode_520_ci";

                    if ((string)column.CollationName != expected)
                    {
                        mismatches.Add(name + " collation " + column.CollationName + ", expected " + expected);
                    }
                }

                foreach (var name in BinaryColumns)
                {
                    if (!found.Contains(name))
                    {
                        mismatches.Add("missing " + name);
                    }
                }

                Assert.True(mismatches.Count == 0, string.Join("; ", mismatches));
            }
        }

        [Fact, CleanDatabase]
        public void JobQueue_Queue_ComparesCaseInsensitively()
        {
            using (var connection = ConnectionUtils.CreateConnection())
            {
                connection.Execute(
                    "insert into `JobQueue` (JobId, Queue) values (1, 'Default')");

                var matches = connection.Query<int>(
                    "select count(*) from `JobQueue` where `Queue` = 'default'").Single();

                Assert.Equal(1, matches);
            }
        }

        [Fact, CleanDatabase]
        public void DistributedLock_Resource_ComparesCaseSensitively()
        {
            using (var connection = ConnectionUtils.CreateConnection())
            {
                connection.Execute(
                    "insert into `DistributedLock` (Resource, CreatedAt) " +
                    "values ('Lock', UTC_TIMESTAMP()), ('lock', UTC_TIMESTAMP())");

                var matches = connection.Query<int>(
                    "select count(*) from `DistributedLock` where Resource = 'lock'").Single();

                Assert.Equal(1, matches);
            }
        }

        [Fact, CleanDatabase]
        public void Install_CreatesSetScoreAsDouble()
        {
            using (var connection = ConnectionUtils.CreateConnection())
            {
                var columnType = connection.Query<string>(
                    "select column_type from information_schema.columns " +
                    "where table_schema = database() and table_name = 'Set' and column_name = 'Score'").Single();

                Assert.Equal("double", columnType);
            }
        }
    }
}
