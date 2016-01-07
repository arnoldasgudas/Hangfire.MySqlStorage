using System;
using System.Collections.Generic;
using System.Linq;
using System.Transactions;
using Dapper;
using Hangfire.Common;
using Hangfire.States;
using Hangfire.Storage;
using MySql.Data.MySqlClient;

namespace Hangfire.MySql
{
    public class MySqlWriteOnlyTransaction : JobStorageTransaction
    {
        private readonly MySqlStorage _storage;

        private readonly Queue<Action<MySqlConnection>> _commandQueue
            = new Queue<Action<MySqlConnection>>();
        private readonly SortedSet<string> _lockedResources = new SortedSet<string>();

        public MySqlWriteOnlyTransaction(MySqlStorage storage)
        {
            if (storage == null) throw new ArgumentNullException("storage");

            _storage = storage;
        }

        public override void ExpireJob(string jobId, TimeSpan expireIn)
        {
            QueueCommand(x => 
                x.Execute(
                    "update Job set ExpireAt = @expireAt where Id = @id",
                    new { expireAt = DateTime.UtcNow.Add(expireIn), id = jobId }));
        }

        public override void PersistJob(string jobId)
        {
            QueueCommand(x => 
                x.Execute(
                    "update Job set ExpireAt = NULL where Id = @id",
                    new { id = jobId }));
        }

        public override void SetJobState(string jobId, IState state)
        {
            QueueCommand(x => x.Execute(
                "insert into State (JobId, Name, Reason, CreatedAt, Data) " +
                "values (@jobId, @name, @reason, @createdAt, @data); " +
                "update Job set StateId = last_insert_id(), StateName = @name where Id = @id;",
                new
                {
                    jobId = jobId,
                    name = state.Name,
                    reason = state.Reason,
                    createdAt = DateTime.UtcNow,
                    data = JobHelper.ToJson(state.SerializeData()),
                    id = jobId
                }));
        }

        public override void AddJobState(string jobId, IState state)
        {
            QueueCommand(x => x.Execute(
                "insert into State (JobId, Name, Reason, CreatedAt, Data) " +
                "values (@jobId, @name, @reason, @createdAt, @data)",
                new
                {
                    jobId = jobId,
                    name = state.Name,
                    reason = state.Reason,
                    createdAt = DateTime.UtcNow,
                    data = JobHelper.ToJson(state.SerializeData())
                }));
        }

        public override void AddToQueue(string queue, string jobId)
        {
            var provider = _storage.QueueProviders.GetProvider(queue);
            var persistentQueue = provider.GetJobQueue();

            QueueCommand(x => persistentQueue.Enqueue(x, queue, jobId));
        }

        public override void IncrementCounter(string key)
        {
            QueueCommand(x => 
                x.Execute(
                    "insert into Counter (`Key`, `Value`) values (@key, @value)",
                    new { key, value = +1 }));
            
        }

        public override void IncrementCounter(string key, TimeSpan expireIn)
        {
            QueueCommand(x => 
                x.Execute(
                    "insert into Counter (`Key`, `Value`, `ExpireAt`) values (@key, @value, @expireAt)",
                    new { key, value = +1, expireAt = DateTime.UtcNow.Add(expireIn) }));
        }

        public override void DecrementCounter(string key)
        {
            QueueCommand(x => 
                x.Execute(
                    "insert into Counter (`Key`, `Value`) values (@key, @value)",
                    new { key, value = -1 }));
        }

        public override void DecrementCounter(string key, TimeSpan expireIn)
        {
            QueueCommand(x => 
                x.Execute(
                    "insert into Counter (`Key`, `Value`, `ExpireAt`) values (@key, @value, @expireAt)",
                    new { key, value = -1, expireAt = DateTime.UtcNow.Add(expireIn) }));
        }

        public override void AddToSet(string key, string value)
        {
            AddToSet(key, value, 0.0);
        }

        public override void AddToSet(string key, string value, double score)
        {
            AcquireSetLock();
            QueueCommand(x => x.Execute(
                "INSERT INTO `Set` (`Key`, `Value`, `Score`) " +
                "VALUES (@Key, @Value, @Score) " +
                "ON DUPLICATE KEY UPDATE `Score` = @Score",
                new { key, value, score }));
        }

        public override void AddRangeToSet(string key, IList<string> items)
        {
            if (key == null) throw new ArgumentNullException("key");
            if (items == null) throw new ArgumentNullException("items");

            AcquireSetLock();
            QueueCommand(x => 
                x.Execute(
                    "insert into `Set` (`Key`, Value, Score) values (@key, @value, 0.0)", 
                    items.Select(value => new { key = key, value = value }).ToList()));
        }


        public override void RemoveFromSet(string key, string value)
        {
            AcquireSetLock();
            QueueCommand(x => x.Execute(
                "delete from `Set` where `Key` = @key and Value = @value",
                new { key, value }));
        }

        public override void ExpireSet(string key, TimeSpan expireIn)
        {
            if (key == null) throw new ArgumentNullException("key");

            AcquireSetLock();
            QueueCommand(x => 
                x.Execute(
                    "update `Set` set ExpireAt = @expireAt where `Key` = @key", 
                    new { key = key, expireAt = DateTime.UtcNow.Add(expireIn) }));
        }

        public override void InsertToList(string key, string value)
        {
            AcquireListLock();
            QueueCommand(x => x.Execute(
                "insert into List (`Key`, Value) values (@key, @value)",
                new { key, value }));
        }


        public override void ExpireList(string key, TimeSpan expireIn)
        {
            if (key == null) throw new ArgumentNullException("key");

            AcquireListLock();
            QueueCommand(x => 
                x.Execute(
                    "update List set ExpireAt = @expireAt where `Key` = @key", 
                    new { key = key, expireAt = DateTime.UtcNow.Add(expireIn) }));
        }

        public override void RemoveFromList(string key, string value)
        {
            AcquireListLock();
            QueueCommand(x => x.Execute(
                "delete from List where `Key` = @key and Value = @value",
                new { key, value }));
        }

        public override void TrimList(string key, int keepStartingFrom, int keepEndingAt)
        {
            AcquireListLock();
            QueueCommand(x => x.Execute(
                @"delete lst
from List lst
	inner join (SELECT tmp.Id, @rownum := @rownum + 1 AS rank
		  		FROM List tmp, 
       				(SELECT @rownum := 0) r ) ranked on ranked.Id = lst.Id
where lst.Key = @key
    and ranked.rank not between @start and @end",
                new { key = key, start = keepStartingFrom + 1, end = keepEndingAt + 1 }));
        }

        public override void PersistHash(string key)
        {
            if (key == null) throw new ArgumentNullException("key");

            AcquireHashLock();
            QueueCommand(x => x.Execute("update Hash set ExpireAt = null where `Key` = @key", new { key = key }));
        }

        public override void PersistSet(string key)
        {
            if (key == null) throw new ArgumentNullException("key");

            AcquireSetLock();
            QueueCommand(x => x.Execute("update `Set` set ExpireAt = null where `Key` = @key", new { key = key }));
        }

        public override void RemoveSet(string key)
        {
            if (key == null) throw new ArgumentNullException("key");

            AcquireSetLock();
            QueueCommand(x => x.Execute("delete from `Set` where `Key` = @key", new { key = key }));
        }

        public override void PersistList(string key)
        {
            if (key == null) throw new ArgumentNullException("key");

            AcquireListLock();
            QueueCommand(x => x.Execute("update List set ExpireAt = null where `Key` = @key", new { key = key }));
        }

        public override void SetRangeInHash(string key, IEnumerable<KeyValuePair<string, string>> keyValuePairs)
        {
            if (key == null) throw new ArgumentNullException("key");
            if (keyValuePairs == null) throw new ArgumentNullException("keyValuePairs");

            AcquireHashLock();
            QueueCommand(x => 
                x.Execute(
                    "insert into Hash (`Key`, Field, Value) " +
                    "values (@key, @field, @value) " +
                    "on duplicate key update Value = @value",
                    keyValuePairs.Select(y => new { key = key, field = y.Key, value = y.Value })));
        }

        public override void ExpireHash(string key, TimeSpan expireIn)
        {
            if (key == null) throw new ArgumentNullException("key");

            AcquireHashLock();
            QueueCommand(x => 
                x.Execute(
                    "update `Hash` set ExpireAt = @expireAt where `Key` = @key", 
                    new { key = key, expireAt = DateTime.UtcNow.Add(expireIn) }));
        }

        public override void RemoveHash(string key)
        {
            if (key == null) throw new ArgumentNullException("key");

            AcquireHashLock();
            QueueCommand(x => x.Execute("delete from Hash where `Key` = @key", new { key }));
        }

        public override void Commit()
        {
            _storage.UseTransaction(connection =>
            {
                connection.EnlistTransaction(Transaction.Current);
                
                //todo: lock resources
                //if (_lockedResources.Count > 0)
                //{
                //    connection.Execute(
                //        "set nocount on;" +
                //        "exec sp_getapplock @Resource=@resource, @LockMode=N'Exclusive'",
                //        _lockedResources.Select(x => new { resource = x }));
                //}

                foreach (var command in _commandQueue)
                {
                    command(connection);
                }
            });
        }

        internal void QueueCommand(Action<MySqlConnection> action)
        {
            _commandQueue.Enqueue(action);
        }

        private void AcquireSetLock()
        {
            AcquireLock(String.Format("Hangfire:Set:Lock"));
        }
        
        private void AcquireListLock()
        {
            AcquireLock(String.Format("Hangfire:List:Lock"));
        }

        private void AcquireHashLock()
        {
            AcquireLock(String.Format("Hangfire:Hash:Lock"));
        }

        private void AcquireLock(string resource)
        {
            _lockedResources.Add(resource);
        }
    }
}
