using System;

namespace Hangfire.MySql.JobQueue
{
    internal class MySqlJobQueueProvider : IPersistentJobQueueProvider
    {
        private readonly IPersistentJobQueue _jobQueue;
        private readonly IPersistentJobQueueMonitoringApi _monitoringApi;

        public MySqlJobQueueProvider(MySqlStorage storage, MySqlStorageOptions options)
        {
            if (storage == null) throw new ArgumentNullException("storage");
            if (options == null) throw new ArgumentNullException("options");

            _jobQueue = new MySqlJobQueue(storage, options);
            _monitoringApi = new MySqlJobQueueMonitoringApi(storage);
        }

        public IPersistentJobQueue GetJobQueue()
        {
            return _jobQueue;
        }

        public IPersistentJobQueueMonitoringApi GetJobQueueMonitoringApi()
        {
            return _monitoringApi;
        }
    }
}