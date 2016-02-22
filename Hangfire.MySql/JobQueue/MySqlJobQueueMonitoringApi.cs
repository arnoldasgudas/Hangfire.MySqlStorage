using System;
using System.Collections.Generic;
using System.Linq;
using System.Transactions;
using Dapper;
using MySql.Data.MySqlClient;

namespace Hangfire.MySql.JobQueue
{
    internal class MySqlJobQueueMonitoringApi : IPersistentJobQueueMonitoringApi
    {
        private static readonly TimeSpan QueuesCacheTimeout = TimeSpan.FromSeconds(5);
        private readonly object _cacheLock = new object();
        private List<string> _queuesCache = new List<string>();
        private DateTime _cacheUpdated;

        private readonly MySqlStorage _storage;
        public MySqlJobQueueMonitoringApi(MySqlStorage storage)
        {
            if (storage == null) throw new ArgumentNullException("storage");
            _storage = storage;
        }

        public IEnumerable<string> GetQueues()
        {
            lock (_cacheLock)
            {
                if (_queuesCache.Count == 0 || _cacheUpdated.Add(QueuesCacheTimeout) < DateTime.UtcNow)
                {
                    var result = UseTransaction(connection =>
                    {
                        return connection.Query("select distinct(Queue) from JobQueue").Select(x => (string)x.Queue).ToList();
                    });

                    _queuesCache = result;
                    _cacheUpdated = DateTime.UtcNow;
                }

                return _queuesCache.ToList();
            }
        }

        public IEnumerable<int> GetEnqueuedJobIds(string queue, int @from, int perPage)
        {
            string sqlQuery = @"
SET @rank=0;
select r.JobId from (
  select jq.JobId, @rank := @rank+1 AS rank 
  from JobQueue jq
  where jq.Queue = @queue
  order by jq.Id
) as r
where r.rank between @start and @end;";

            return UseTransaction(connection =>
                connection.Query<int>(
                    sqlQuery,
                    new {queue = queue, start = @from + 1, end = @from + perPage}));
        }

        public IEnumerable<int> GetFetchedJobIds(string queue, int @from, int perPage)
        {
            return Enumerable.Empty<int>();
        }

        public EnqueuedAndFetchedCountDto GetEnqueuedAndFetchedCount(string queue)
        {
            return UseTransaction(connection =>
            {
                var result = 
                    connection.Query<int>(
                        "select count(Id) from JobQueue where Queue = @queue", new { queue = queue }).Single();

                return new EnqueuedAndFetchedCountDto
                {
                    EnqueuedCount = result,
                };
            });
        }

        private T UseTransaction<T>(Func<MySqlConnection, T> func)
        {
            return _storage.UseTransaction(func, IsolationLevel.ReadUncommitted);
        }
    }
}