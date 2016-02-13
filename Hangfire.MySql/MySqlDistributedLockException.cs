using System;

namespace Hangfire.MySql
{
    public class MySqlDistributedLockException : Exception
    {
        public MySqlDistributedLockException(string message) : base(message)
        {
        }
    }
}
