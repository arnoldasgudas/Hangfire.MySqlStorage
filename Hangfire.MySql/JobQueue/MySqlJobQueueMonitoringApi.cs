using System;
using System.Collections.Generic;

namespace Hangfire.MySql.JobQueue
{
    internal class MySqlJobQueueMonitoringApi : IPersistentJobQueueMonitoringApi
    {
        private readonly MySqlStorage _storage;
        public MySqlJobQueueMonitoringApi(MySqlStorage storage)
        {
            _storage = storage;
        }

        public IEnumerable<string> GetQueues()
        {
            throw new NotImplementedException();
        }

        public IEnumerable<int> GetEnqueuedJobIds(string queue, int @from, int perPage)
        {
            throw new NotImplementedException();
        }

        public IEnumerable<int> GetFetchedJobIds(string queue, int @from, int perPage)
        {
            throw new NotImplementedException();
        }

        public EnqueuedAndFetchedCountDto GetEnqueuedAndFetchedCount(string queue)
        {
            throw new NotImplementedException();
        }
    }
}