using System;
using System.Data;
using System.Globalization;
using Dapper;
using Hangfire.Logging;
using Hangfire.Storage;

namespace Hangfire.MySql.JobQueue
{
    internal class MySqlFetchedJob : IFetchedJob
    {
        private static readonly ILog Logger = LogProvider.GetLogger(typeof(MySqlFetchedJob));

        private readonly MySqlStorage _storage;
        private readonly IDbConnection _connection;
        private readonly int _id;
        private bool _removedFromQueue;
        private bool _requeued;
        private bool _disposed;

        public MySqlFetchedJob(
            MySqlStorage storage, 
            IDbConnection connection,
            FetchedJob fetchedJob)
        {
            if (storage == null) throw new ArgumentNullException("storage");
            if (connection == null) throw new ArgumentNullException("connection");
            if (fetchedJob == null) throw new ArgumentNullException("fetchedJob");

            _storage = storage;
            _connection = connection;
            _id = fetchedJob.Id;
            JobId = fetchedJob.JobId.ToString(CultureInfo.InvariantCulture);
            Queue = fetchedJob.Queue; 
        }

        public void Dispose()
        {

            if (_disposed) return;

            if (!_removedFromQueue && !_requeued)
            {
                Requeue();
            }

            _storage.ReleaseConnection(_connection);

            _disposed = true;
        }

        public void RemoveFromQueue()
        {
            Logger.TraceFormat("RemoveFromQueue JobId={0}", JobId);

            //todo: unit test
            _connection.Execute(
                "delete from JobQueue " +
                "where Id = @id",
                new
                {
                    id = _id
                });

            _removedFromQueue = true;
        }

        public void Requeue()
        {
            Logger.TraceFormat("Requeue JobId={0}", JobId);

            //todo: unit test
            _connection.Execute(
                "update JobQueue set FetchedAt = null " +
                "where Id = @id",
                new
                {
                    id = _id
                });
            _requeued = true;
        }

        public string JobId { get; private set; }

        public string Queue { get; private set; }
    }
}