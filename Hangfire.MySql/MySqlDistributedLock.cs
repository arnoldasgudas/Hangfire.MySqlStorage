using System;
using Hangfire.Logging;

namespace Hangfire.MySql
{
    public class MySqlDistributedLock : IDisposable
    {
        private static readonly ILog Logger = LogProvider.GetCurrentClassLogger();

        public MySqlDistributedLock(MySqlStorage storage, string resource, TimeSpan timeout)
        {
            Logger.Warn("Method MySqlDistributedLock not implemented");
            //todo: implement
        }

        public void Dispose()
        {
            Logger.Warn("Method Dispose not implemented");
            //todo: implement
        }
    }
}