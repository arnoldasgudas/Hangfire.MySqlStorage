using System;
using System.Linq;
using System.Threading;
using Dapper;
using Hangfire.MySql.JobQueue;
using Moq;
using MySql.Data.MySqlClient;
using Xunit;

namespace Hangfire.MySql.Tests.JobQueue
{
    public class MySqlJobQueueTests : IClassFixture<TestDatabaseFixture>, IDisposable
    {
        private static readonly string[] DefaultQueues = { "default" };
        private readonly MySqlStorage _storage;
        private readonly MySqlConnection _connection;

        public MySqlJobQueueTests()
        {
            _connection = ConnectionUtils.CreateConnection();
            _storage = new MySqlStorage(_connection);
        }

        public void Dispose()
        {
            _connection.Dispose();
            _storage.Dispose();
        }

        [Fact, CleanDatabase]
        public void Ctor_ThrowsAnException_WhenStorageIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => new MySqlJobQueue(null, new MySqlStorageOptions()));

            Assert.Equal("storage", exception.ParamName);
        }

        [Fact, CleanDatabase]
        public void Ctor_ThrowsAnException_WhenOptionsValueIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => new MySqlJobQueue(_storage, null));

            Assert.Equal("options", exception.ParamName);
        }

        [Fact, CleanDatabase]
        public void Dequeue_ShouldThrowAnException_WhenQueuesCollectionIsNull()
        {
            _storage.UseConnection(connection =>
            {
                var queue = CreateJobQueue(connection);

                var exception = Assert.Throws<ArgumentNullException>(
                    () => queue.Dequeue(null, CreateTimingOutCancellationToken()));

                Assert.Equal("queues", exception.ParamName);
            });
        }

        [Fact, CleanDatabase]
        public void Dequeue_ShouldThrowAnException_WhenQueuesCollectionIsEmpty()
        {
            _storage.UseConnection(connection =>
            {
                var queue = CreateJobQueue(connection);

                var exception = Assert.Throws<ArgumentException>(
                    () => queue.Dequeue(new string[0], CreateTimingOutCancellationToken()));

                Assert.Equal("queues", exception.ParamName);
            });
        }

        [Fact, CleanDatabase]
        public void Dequeue_ThrowsOperationCanceled_WhenCancellationTokenIsSetAtTheBeginning()
        {
            _storage.UseConnection(connection =>
            {
                var cts = new CancellationTokenSource();
                cts.Cancel();
                var queue = CreateJobQueue(connection);

                Assert.Throws<OperationCanceledException>(
                    () => queue.Dequeue(DefaultQueues, cts.Token));
            });
        }

        [Fact, CleanDatabase]
        public void Dequeue_ShouldWaitIndefinitely_WhenThereAreNoJobs()
        {
            _storage.UseConnection(connection =>
            {
                var cts = new CancellationTokenSource(200);
                var queue = CreateJobQueue(connection);

                Assert.Throws<OperationCanceledException>(
                    () => queue.Dequeue(DefaultQueues, cts.Token));
            });
        }

        [Fact, CleanDatabase]
        public void Dequeue_ShouldFetchAJob_FromTheSpecifiedQueue()
        {
            const string arrangeSql = @"
insert into JobQueue (JobId, Queue)
values (@jobId, @queue);
select last_insert_id() as Id;";

            // Arrange
            _storage.UseConnection(connection =>
            {
                var id = (int)connection.Query(
                    arrangeSql,
                    new { jobId = 1, queue = "default" }).Single().Id;
                var queue = CreateJobQueue(connection);

                // Act
                var payload = (MySqlFetchedJob)queue.Dequeue(
                    DefaultQueues,
                    CreateTimingOutCancellationToken());

                // Assert
                Assert.Equal("1", payload.JobId);
                Assert.Equal("default", payload.Queue);
            });
        }

        [Fact,CleanDatabase]
        public void Dequeue_ShouldDeleteAJob()
        {
            const string arrangeSql = @"
delete from JobQueue;
delete from Job;
insert into Job (InvocationData, Arguments, CreatedAt)
values (@invocationData, @arguments, UTC_TIMESTAMP());
insert into JobQueue (JobId, Queue)
values (last_insert_id(), @queue)";

            // Arrange
            _storage.UseConnection(connection =>
            {
                connection.Execute(
                    arrangeSql,
                    new { invocationData = "", arguments = "", queue = "default" });
                var queue = CreateJobQueue(connection);

                // Act
                var payload = queue.Dequeue(
                    DefaultQueues,
                    CreateTimingOutCancellationToken());

                payload.RemoveFromQueue();

                // Assert
                Assert.NotNull(payload);

                var jobInQueue = connection.Query("select * from JobQueue").SingleOrDefault();
                Assert.Null(jobInQueue);
            });
        }

        [Fact,CleanDatabase]
        public void Dequeue_ShouldFetchATimedOutJobs_FromTheSpecifiedQueue()
        {
            const string arrangeSql = @"
insert into Job (InvocationData, Arguments, CreatedAt)
values (@invocationData, @arguments, UTC_TIMESTAMP());
insert into JobQueue (JobId, Queue, FetchedAt)
values (last_insert_id(), @queue, @fetchedAt)";

            // Arrange
            _storage.UseConnection(connection =>
            {
                connection.Execute(
                    arrangeSql,
                    new
                    {
                        queue = "default",
                        fetchedAt = DateTime.UtcNow.AddDays(-1),
                        invocationData = "",
                        arguments = ""
                    });
                var queue = CreateJobQueue(connection);

                // Act
                var payload = queue.Dequeue(
                    DefaultQueues,
                    CreateTimingOutCancellationToken());

                // Assert
                Assert.NotEmpty(payload.JobId);
            });
        }

        [Fact,CleanDatabase]
        public void Dequeue_ShouldSetFetchedAt_OnlyForTheFetchedJob()
        {
            const string arrangeSql = @"
insert into Job (InvocationData, Arguments, CreatedAt)
values (@invocationData, @arguments, UTC_TIMESTAMP());
insert into JobQueue (JobId, Queue)
values (last_insert_id(), @queue)";

            // Arrange
            _storage.UseConnection(connection =>
            {
                connection.Execute("delete from JobQueue; delete from Job;");

                connection.Execute(
                    arrangeSql,
                    new[]
                    {
                        new { queue = "default", invocationData = "", arguments = "" },
                        new { queue = "default", invocationData = "", arguments = "" }
                    });
                var queue = CreateJobQueue(connection);

                // Act
                var payload = queue.Dequeue(
                    DefaultQueues,
                    CreateTimingOutCancellationToken());

                // Assert
                var otherJobFetchedAt = connection.Query<DateTime?>(
                    "select FetchedAt from JobQueue where JobId != @id",
                    new { id = payload.JobId }).Single();

                Assert.Null(otherJobFetchedAt);
            });
        }

        [Fact, CleanDatabase]
        public void Dequeue_ShouldFetchJobs_OnlyFromSpecifiedQueues()
        {
            const string arrangeSql = @"
insert into Job (InvocationData, Arguments, CreatedAt)
values (@invocationData, @arguments, UTC_TIMESTAMP());
insert into JobQueue (JobId, Queue)
values (last_insert_id(), @queue)";

            _storage.UseConnection(connection =>
            {
                connection.Execute("delete from JobQueue; delete from Job;");
                var queue = CreateJobQueue(connection);

                connection.Execute(
                    arrangeSql,
                    new { queue = "critical", invocationData = "", arguments = "" });

                Assert.Throws<OperationCanceledException>(
                    () => queue.Dequeue(
                        DefaultQueues,
                        CreateTimingOutCancellationToken()));
            });
        }

        [Fact, CleanDatabase]
        public void Dequeue_ShouldFetchJobs_FromMultipleQueues()
        {
            const string arrangeSql = @"
insert into Job (InvocationData, Arguments, CreatedAt)
values (@invocationData, @arguments, UTC_TIMESTAMP());
insert into JobQueue (JobId, Queue)
values (last_insert_id(), @queue)";

            _storage.UseConnection(connection =>
            {
                connection.Execute(
                    arrangeSql,
                    new[]
                    {
                        new { queue = "default", invocationData = "", arguments = "" },
                        new { queue = "critical", invocationData = "", arguments = "" }
                    });

                var queue = CreateJobQueue(connection);

                var critical = (MySqlFetchedJob)queue.Dequeue(
                    new[] { "critical", "default" },
                    CreateTimingOutCancellationToken());

                Assert.NotNull(critical.JobId);
                Assert.Equal("critical", critical.Queue);

                var @default = (MySqlFetchedJob)queue.Dequeue(
                    new[] { "critical", "default" },
                    CreateTimingOutCancellationToken());

                Assert.NotNull(@default.JobId);
                Assert.Equal("default", @default.Queue);
            });
        }

        [Fact, CleanDatabase]
        public void Enqueue_AddsAJobToTheQueue()
        {
            _storage.UseConnection(connection =>
            {
                connection.Execute("delete from JobQueue");

                var queue = CreateJobQueue(connection);

                queue.Enqueue(connection, "default", "1");

                var record = connection.Query("select * from JobQueue").Single();
                Assert.Equal("1", record.JobId.ToString());
                Assert.Equal("default", record.Queue);
                Assert.Null(record.FetchedAt);
            });
        }

        private static CancellationToken CreateTimingOutCancellationToken()
        {
            var source = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            return source.Token;
        }

        public static void Sample(string arg1, string arg2) { }

        private static MySqlJobQueue CreateJobQueue(MySqlConnection connection)
        {
            var storage = new MySqlStorage(connection);
            return new MySqlJobQueue(storage, new MySqlStorageOptions());
        }
    }
}
