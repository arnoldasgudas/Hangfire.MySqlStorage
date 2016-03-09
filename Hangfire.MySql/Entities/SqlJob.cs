using System;

namespace Hangfire.MySql.Entities
{
    internal class SqlJob
    {
        public int Id { get; set; }
        public string InvocationData { get; set; }
        public string Arguments { get; set; }
        public DateTime CreatedAt { get; set; }
        public DateTime ExpireAt { get; set; }

        public DateTime? FetchedAt { get; set; }

        public string StateName { get; set; }
        public string StateReason { get; set; }
        public string StateData { get; set; }
    }
}