using System;

namespace Hangfire.MySql
{
    public class MySqlDistributedLock : IDisposable
    {
        public MySqlDistributedLock(MySqlStorage storage, string resource, TimeSpan timeout)
        {
            //todo: implement
        }

        public void Dispose()
        {
            //todo: implement
        }
    }
}