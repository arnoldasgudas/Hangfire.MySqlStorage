using System;
using System.Linq;
using System.Threading;
using Dapper;
using MySql.Data.MySqlClient;
using Xunit;

namespace Hangfire.MySql.Tests
{
    public class CountersAggregatorTests : IClassFixture<TestDatabaseFixture>
    {
        [Fact]
        public void CountersAggregatorExecutesProperly()
        {
            const string createSql = @"
insert into Counter (`Key`, Value, ExpireAt) 
values ('key', 1, @expireAt)";

            using (var connection = CreateConnection())
            {
                // Arrange
                connection.Execute(createSql, new { expireAt = DateTime.UtcNow.AddHours(1) });

                var aggregator = CreateAggregator(connection);
                var cts = new CancellationTokenSource();
                cts.Cancel();

                // Act
                aggregator.Execute(cts.Token);

                // Assert
                Assert.Equal(1, connection.Query<int>(@"select count(*) from AggregatedCounter").Single());
            }
        }

        private static MySqlConnection CreateConnection()
        {
            return ConnectionUtils.CreateConnection();
        }

        private static CountersAggregator CreateAggregator(MySqlConnection connection)
        {
            var storage = new MySqlStorage(connection);
            return new CountersAggregator(storage, TimeSpan.Zero);
        }
    }
}
