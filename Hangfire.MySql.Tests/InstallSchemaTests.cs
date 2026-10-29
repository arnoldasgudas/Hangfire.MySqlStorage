using System.Linq;
using Dapper;
using Xunit;

namespace Hangfire.MySql.Tests
{
    public class InstallSchemaTests : IClassFixture<TestDatabaseFixture>
    {
        [Fact, CleanDatabase]
        public void Install_CreatesEveryStringColumnAsUtf8mb4Unicode520()
        {
            using (var connection = ConnectionUtils.CreateConnection())
            {
                var charsetsAndCollations = connection.Query<string>(
                    "select distinct concat(character_set_name, '/', collation_name) " +
                    "from information_schema.columns " +
                    "where table_schema = database() and character_set_name is not null").ToList();

                Assert.Equal(new[] { "utf8mb4/utf8mb4_unicode_520_ci" }, charsetsAndCollations);
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
