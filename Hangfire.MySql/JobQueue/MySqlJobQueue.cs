using System;
using System.Data;
using System.Globalization;
using System.Threading;
using Dapper;
using Hangfire.Annotations;
using Hangfire.Storage;
using MySql.Data.MySqlClient;

namespace Hangfire.MySql.JobQueue
{
    internal class MySqlJobQueue : IPersistentJobQueue
    {
        private readonly MySqlStorage _storage;
        private readonly MySqlStorageOptions _options;
        public MySqlJobQueue(MySqlStorage storage, MySqlStorageOptions options)
        {
            _storage = storage;
            _options = options;
        }

        public IFetchedJob Dequeue(string[] queues, CancellationToken cancellationToken)
        {
            if (queues == null) throw new ArgumentNullException("queues");
            if (queues.Length == 0) throw new ArgumentException("Queue array must be non-empty.", "queues");

            FetchedJob fetchedJob = null;
            MySqlConnection connection = null;
            MySqlTransaction transaction = null;
            int jobQueueId = 0;

            do
            {
                cancellationToken.ThrowIfCancellationRequested();

                connection = _storage.CreateAndOpenConnection();
                transaction = connection.BeginTransaction(IsolationLevel.ReadCommitted);

                try
                {
                    var cmd = connection.CreateCommand();
                    cmd.Parameters.AddWithValue("@timeout", _options.InvisibilityTimeout.Negate().TotalSeconds);

                    var parameters = new string[queues.Length];
                    foreach (var queue in queues)
                    {
                        var i = Array.IndexOf(queues, queue);
                        parameters[i] = string.Format("@queue{0}", i);
                        cmd.Parameters.AddWithValue(parameters[i], queues[i]);
                    }
                    
                     cmd.CommandText = 
                         string.Format(
                             "select Id, JobId, Queue " +
                             "from JobQueue " +
                             "where (FetchedAt is null or FetchedAt < DATE_ADD(UTC_TIMESTAMP(), INTERVAL @timeout SECOND)) " +
                             "   and Queue in ({0}) " +
                             "limit 1",
                             string.Join(",", parameters)); ;

                    using (var reader = cmd.ExecuteReader())
                    {
                        if (reader.Read())
                        {
                            jobQueueId = reader.GetInt32("Id");
                            fetchedJob =
                                new FetchedJob
                                {
                                    Id = jobQueueId,
                                    JobId = reader.GetInt32("JobId"),
                                    Queue = reader.GetString("Queue")
                                };
                        }
                    }

                    if (jobQueueId > 0)
                    {
                        cmd.CommandText = "delete from JobQueue where Id = @Id ";
                        cmd.Parameters.AddWithValue("@Id", jobQueueId);
                        cmd.ExecuteNonQuery();
                    }
                }
                catch (MySqlException)
                {
                    transaction.Dispose();
                    _storage.ReleaseConnection(connection);
                    throw;
                }

                if (fetchedJob == null)
                {
                    transaction.Rollback();
                    transaction.Dispose();
                    _storage.ReleaseConnection(connection);

                    cancellationToken.WaitHandle.WaitOne(_options.QueuePollInterval);
                    cancellationToken.ThrowIfCancellationRequested();
                }
            } while (fetchedJob == null);

            return new MySqlFetchedJob(
                _storage,
                connection,
                transaction,
                fetchedJob.JobId.ToString(CultureInfo.InvariantCulture),
                fetchedJob.Queue);
        }

        public void Enqueue(IDbConnection connection, string queue, string jobId)
        {
            connection.Execute("insert into JobQueue (JobId, Queue) values (@jobId, @queue)", new { jobId, queue });
        }

        [UsedImplicitly(ImplicitUseTargetFlags.WithMembers)]
        private class FetchedJob
        {
            public int Id { get; set; }
            public int JobId { get; set; }
            public string Queue { get; set; }
        }
    }
}