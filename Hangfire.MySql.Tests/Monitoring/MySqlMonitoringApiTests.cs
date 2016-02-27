using System;
using System.Collections.Generic;
using System.Transactions;
using Dapper;
using Hangfire.MySql.JobQueue;
using Hangfire.MySql.Monitoring;
using Hangfire.Storage.Monitoring;
using Moq;
using MySql.Data.MySqlClient;
using Xunit;

namespace Hangfire.MySql.Tests.Monitoring
{
    public class MySqlMonitoringApiTests : IClassFixture<TestDatabaseFixture>, IDisposable
    {
        private readonly MySqlMonitoringApi _sut;
        private readonly MySqlStorage _storage;
        private readonly int? _jobListLimit = 1000;
        private readonly MySqlConnection _connection;
        private readonly string _invocationData =
            "{\"Type\":\"System.Console, mscorlib, Version=4.0.0.0, Culture=neutral, PublicKeyToken=b77a5c561934e089\"," +
            "\"Method\":\"WriteLine\"," +
            "\"ParameterTypes\":\"[\\\"System.String, mscorlib, Version=4.0.0.0, Culture=neutral, PublicKeyToken=b77a5c561934e089\\\"]\"," +
            "\"Arguments\":\"[\\\"\\\"test\\\"\\\"]\"}";
        private readonly string _arguments = "[\"test\"]";
        private readonly DateTime _createdAt = DateTime.UtcNow;
        private readonly DateTime _expireAt = DateTime.UtcNow.AddMinutes(1);

        public MySqlMonitoringApiTests()
        {
            _connection = new MySqlConnection(ConnectionUtils.GetConnectionString());
            _connection.Open();

            var persistentJobQueueMonitoringApiMock = new Mock<IPersistentJobQueueMonitoringApi>();
            persistentJobQueueMonitoringApiMock.Setup(m => m.GetQueues()).Returns(new[] {"default"});

            var defaultProviderMock = new Mock<IPersistentJobQueueProvider>();
            defaultProviderMock.Setup(m => m.GetJobQueueMonitoringApi())
                .Returns(persistentJobQueueMonitoringApiMock.Object);

            var mySqlStorageMock = new Mock<MySqlStorage>(_connection);
            mySqlStorageMock
                .Setup(m => m.QueueProviders)
                .Returns(new PersistentJobQueueProviderCollection(defaultProviderMock.Object));

            _storage = mySqlStorageMock.Object;
            _sut = new MySqlMonitoringApi(_storage, _jobListLimit);
        }

        public void Dispose()
        {
            _connection.Dispose();
            _storage.Dispose();
        }

        [Fact, CleanDatabase(IsolationLevel.ReadUncommitted)]
        public void GetStatistics_ShouldReturnEnqueuedCount()
        {
            const int expectedEnqueuedCount = 1;

            StatisticsDto result = null;
            _storage.UseConnection(connection =>
            {
                connection.Execute(
                    "insert into Job (InvocationData, Arguments, CreatedAt, StateName) " +
                    "values ('', '', UTC_TIMESTAMP(),'Enqueued');");

                result = _sut.GetStatistics();
            });

            Assert.Equal(expectedEnqueuedCount, result.Enqueued);
        }

        [Fact, CleanDatabase(IsolationLevel.ReadUncommitted)]
        public void GetStatistics_ShouldReturnFailedCount()
        {
            const int expectedFailedCount = 2;

            StatisticsDto result = null;
            _storage.UseConnection(connection =>
            {
                connection.Execute(
                    "insert into Job (InvocationData, Arguments, CreatedAt, StateName) " +
                    "values ('', '', UTC_TIMESTAMP(),'Failed'), " +
                    "       ('', '', UTC_TIMESTAMP(),'Failed');");

                result = _sut.GetStatistics();
            });

            Assert.Equal(expectedFailedCount, result.Failed);
        }

        [Fact, CleanDatabase(IsolationLevel.ReadUncommitted)]
        public void GetStatistics_ShouldReturnProcessingCount()
        {
            const int expectedProcessingCount = 1;

            StatisticsDto result = null;
            _storage.UseConnection(connection =>
            {
                connection.Execute(
                    "insert into Job (InvocationData, Arguments, CreatedAt, StateName) " +
                    "values ('', '', UTC_TIMESTAMP(),'Processing')");

                result = _sut.GetStatistics();
            });

            Assert.Equal(expectedProcessingCount, result.Processing);
        }

        [Fact, CleanDatabase(IsolationLevel.ReadUncommitted)]
        public void GetStatistics_ShouldReturnScheduledCount()
        {
            const int expectedScheduledCount = 3;

            StatisticsDto result = null;
            _storage.UseConnection(connection =>
            {
                connection.Execute(
                    "insert into Job (InvocationData, Arguments, CreatedAt, StateName) " +
                    "values ('', '', UTC_TIMESTAMP(),'Scheduled')," +
                    "       ('', '', UTC_TIMESTAMP(),'Scheduled')," +
                    "       ('', '', UTC_TIMESTAMP(),'Scheduled');");

                result = _sut.GetStatistics();
            });

            Assert.Equal(expectedScheduledCount, result.Scheduled);
        }

        [Fact, CleanDatabase(IsolationLevel.ReadUncommitted)]
        public void GetStatistics_ShouldReturnQueuesCount()
        {
            const int expectedQueuesCount = 1;

            var result = _sut.GetStatistics();

            Assert.Equal(expectedQueuesCount, result.Queues);
        }

        [Fact, CleanDatabase(IsolationLevel.ReadUncommitted)]
        public void GetStatistics_ShouldReturnServersCount()
        {
            const int expectedServersCount = 2;

            StatisticsDto result = null;
            _storage.UseConnection(connection =>
            {
                connection.Execute(
                    "insert into Server (Id, Data) " +
                    "values (1,'1')," +
                    "       (2,'2'); ");

                result = _sut.GetStatistics();
            });

            Assert.Equal(expectedServersCount, result.Servers);
        }

        [Fact, CleanDatabase(IsolationLevel.ReadUncommitted)]
        public void GetStatistics_ShouldReturnSucceededCount()
        {
            const int expectedStatsSucceededCount = 11;

            StatisticsDto result = null;
            _storage.UseConnection(connection =>
            {
                connection.Execute(
                    "insert into Counter (`Key`,`Value`) " +
                    "values ('stats:succeeded',1); " +
                    "insert into AggregatedCounter (`Key`,`Value`) " +
                    "values ('stats:succeeded',10); ");

                result = _sut.GetStatistics();
            });

            Assert.Equal(expectedStatsSucceededCount, result.Succeeded);
        }

        [Fact, CleanDatabase(IsolationLevel.ReadUncommitted)]
        public void GetStatistics_ShouldReturnDeletedCount()
        {
            const int expectedStatsDeletedCount = 7;

            StatisticsDto result = null;
            _storage.UseConnection(connection =>
            {
                connection.Execute(
                    "insert into AggregatedCounter (`Key`,`Value`) " +
                    "values ('stats:deleted',5); " +
                    "insert into Counter (`Key`,`Value`) " +
                    "values ('stats:deleted',1)," +
                    "       ('stats:deleted',1); ");

                result = _sut.GetStatistics();
            });

            Assert.Equal(expectedStatsDeletedCount, result.Deleted);
        }

        [Fact, CleanDatabase(IsolationLevel.ReadUncommitted)]
        public void GetStatistics_ShouldReturnRecurringCount()
        {
            const int expectedRecurringCount = 1;
            
            StatisticsDto result = null;
            _storage.UseConnection(connection =>
            {
                connection.Execute(
                    "insert into `Set` (Id, `Key`, `Value`, Score) " +
                    "values (1, 'recurring-jobs', 'test', 0);");

                result = _sut.GetStatistics();
            });

            Assert.Equal(expectedRecurringCount, result.Recurring);
        }

        [Fact, CleanDatabase(IsolationLevel.ReadUncommitted)]
        public void JobDetails_ShouldReturnJob()
        {
            JobDetailsDto result = null;

            _storage.UseConnection(connection =>
            {
                var jobId = 
                    connection.ExecuteScalar<string>(
                    "insert into Job (CreatedAt,InvocationData,Arguments,ExpireAt) " +
                    "values (@createdAt, @invocationData, @arguments,@expireAt);" +
                    "select last_insert_id(); ",

                    new { createdAt = _createdAt, invocationData = _invocationData, arguments = _arguments, expireAt = _expireAt });

                result = _sut.JobDetails(jobId);
            });

            Assert.NotNull(result.Job);
        }

        [Fact, CleanDatabase(IsolationLevel.ReadUncommitted)]
        public void JobDetails_ShouldReturnCreatedAtAndExpireAt()
        {
            JobDetailsDto result = null;

            _storage.UseConnection(connection =>
            {
                var jobId =
                    connection.ExecuteScalar<string>(
                    "insert into Job (CreatedAt,InvocationData,Arguments,ExpireAt) " +
                    "values (@createdAt, @invocationData, @arguments,@expireAt);" +
                    "select last_insert_id(); ",
                    new { createdAt = _createdAt, invocationData = _invocationData, arguments = _arguments, expireAt = _expireAt });

                result = _sut.JobDetails(jobId);
            });

            Assert.Equal(_createdAt.ToString("yyyy-MM-dd hh:mm:ss"), result.CreatedAt.Value.ToString("yyyy-MM-dd hh:mm:ss"));
            Assert.Equal(_expireAt.ToString("yyyy-MM-dd hh:mm:ss"), result.ExpireAt.Value.ToString("yyyy-MM-dd hh:mm:ss"));
        }

        [Fact, CleanDatabase(IsolationLevel.ReadUncommitted)]
        public void JobDetails_ShouldReturnProperties()
        {
            var properties = new Dictionary<string, string>();
            properties["CurrentUICulture"] = "en-US";
            properties["CurrentCulture"] = "lt-LT";
           
            JobDetailsDto result = null;

            _storage.UseConnection(connection =>
            {
                var jobId =
                    connection.ExecuteScalar<string>(
                    "insert into Job (CreatedAt,InvocationData,Arguments,ExpireAt) " +
                    "values (@createdAt, @invocationData, @arguments,@expireAt);" +
                    "set @jobId = last_insert_id(); " +
                    "insert into JobParameter (JobId, Name, Value) " +
                    "values (@jobId, 'CurrentUICulture', 'en-US')," +
                    "       (@jobId, 'CurrentCulture', 'lt-LT');" +
                    "select @jobId;",

                    new { createdAt = _createdAt, invocationData = _invocationData, arguments = _arguments, expireAt = _expireAt });

                result = _sut.JobDetails(jobId);
            });

            Assert.Equal(properties, result.Properties);
        }

        [Fact, CleanDatabase(IsolationLevel.ReadUncommitted)]
        public void JobDetails_ShouldReturnHistory()
        {
            const string jobStateName = "Scheduled";
            const string stateData = "{\"EnqueueAt\":\"2016-02-21T11:56:05.0561988Z\", \"ScheduledAt\":\"2016-02-21T11:55:50.0561988Z\"}";

            JobDetailsDto result = null;

            _storage.UseConnection(connection =>
            {
                var jobId =
                    connection.ExecuteScalar<string>(
                    "insert into Job (CreatedAt,InvocationData,Arguments,ExpireAt) " +
                    "values (@createdAt, @invocationData, @arguments,@expireAt);" +
                    "set @jobId = last_insert_id(); " +
                    "insert into State (JobId, Name, CreatedAt, Data) " +
                    "values (@jobId, @jobStateName, @createdAt, @stateData);" +
                    "select @jobId;",

                    new
                    {
                        createdAt = _createdAt, 
                        invocationData = _invocationData, 
                        arguments = _arguments, 
                        expireAt = _expireAt, 
                        jobStateName, 
                        stateData
                    });

                result = _sut.JobDetails(jobId);
            });

            Assert.Equal(1, result.History.Count);
        }
    }
}
