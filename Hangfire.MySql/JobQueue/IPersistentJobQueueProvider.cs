namespace Hangfire.MySql.JobQueue
{
    public interface IPersistentJobQueueProvider
    {
        IPersistentJobQueue GetJobQueue();
        IPersistentJobQueueMonitoringApi GetJobQueueMonitoringApi();
    }
}