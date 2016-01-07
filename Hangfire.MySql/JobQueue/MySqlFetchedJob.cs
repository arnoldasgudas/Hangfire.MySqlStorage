using System;
using System.Data;
using Hangfire.Storage;
using MySql.Data.MySqlClient;

namespace Hangfire.MySql.JobQueue
{
    public class MySqlFetchedJob : IFetchedJob
    {
        private readonly MySqlStorage _storage;
        private readonly IDbConnection _connection;
        private readonly IDbTransaction _transaction;
        public MySqlFetchedJob(MySqlStorage storage, IDbConnection connection, IDbTransaction transaction, string jobId, string queue)
        {
            if (storage == null) throw new ArgumentNullException("storage");
            if (connection == null) throw new ArgumentNullException("connection");
            if (transaction == null) throw new ArgumentNullException("transaction");
            if (jobId == null) throw new ArgumentNullException("jobId");
            if (queue == null) throw new ArgumentNullException("queue");

            _storage = storage;
            _connection = connection;
            _transaction = transaction;

            JobId = jobId;
            Queue = queue;
        }

        public void Dispose()
        {
            _transaction.Dispose();
            _storage.ReleaseConnection(_connection);
        }

        public void RemoveFromQueue()
        {
            _transaction.Commit();
        }

        public void Requeue()
        {
            _transaction.Rollback();
        }

        public string JobId { get; private set; }

        public string Queue { get; private set; }
    }
}