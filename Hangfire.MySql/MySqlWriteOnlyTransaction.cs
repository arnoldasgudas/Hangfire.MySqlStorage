using System;
using System.Collections.Generic;
using System.Data;
using System.Transactions;
using Hangfire.Common;
using Hangfire.States;
using Hangfire.Storage;
using MySql.Data.MySqlClient;

namespace Hangfire.MySql
{
    public class MySqlWriteOnlyTransaction : IWriteOnlyTransaction
    {
        private readonly MySqlStorage _storage;

        private readonly Queue<IDbCommand> _commandQueue = new Queue<IDbCommand>();
        private readonly SortedSet<string> _lockedResources = new SortedSet<string>();

        public MySqlWriteOnlyTransaction(MySqlStorage storage)
        {
            _storage = storage;
        }

        public void Dispose()
        {
        }

        public void ExpireJob(string jobId, TimeSpan expireIn)
        {
            var cmd = new MySqlCommand("update Job set ExpireAt = @expireAt where Id = @id");
            cmd.Parameters.AddWithValue("@expireAt", DateTime.UtcNow.Add(expireIn));
            cmd.Parameters.AddWithValue("@id", jobId);

            QueueCommand(cmd);
            
        }

        public void PersistJob(string jobId)
        {
            var cmd = new MySqlCommand("update Job set ExpireAt = NULL where Id = @id");
            cmd.Parameters.AddWithValue("@id", jobId);
            
            QueueCommand(cmd);
        }

        public void SetJobState(string jobId, IState state)
        {
            var cmd = 
                new MySqlCommand(
                    "insert into State (JobId, Name, Reason, CreatedAt, Data) " +
                    "values (@jobId, @name, @reason, @createdAt, @data); " +
                    "update Job set StateId = last_insert_id(), StateName = @name where Id = @id;");
            cmd.Parameters.AddWithValue("@jobId", jobId);
            cmd.Parameters.AddWithValue("@name", state.Name);
            cmd.Parameters.AddWithValue("@reason", state.Reason);
            cmd.Parameters.AddWithValue("@createdAt", DateTime.UtcNow);
            cmd.Parameters.AddWithValue("@data", JobHelper.ToJson(state.SerializeData()));
            cmd.Parameters.AddWithValue("@id", jobId);
            
            QueueCommand(cmd);
        }

        public void AddJobState(string jobId, IState state)
        {
            var cmd = 
                new MySqlCommand(
                    "insert into State (JobId, Name, Reason, CreatedAt, Data) " +
                    "values (@jobId, @name, @reason, @createdAt, @data)");
            cmd.Parameters.AddWithValue("@jobId", jobId);
            cmd.Parameters.AddWithValue("@name", state.Name);
            cmd.Parameters.AddWithValue("@reason", state.Reason);
            cmd.Parameters.AddWithValue("@createdAt", DateTime.UtcNow);
            cmd.Parameters.AddWithValue("@data", JobHelper.ToJson(state.SerializeData()));

            QueueCommand(cmd);
        }

        public void AddToQueue(string queue, string jobId)
        {

            var provider = _storage.QueueProviders.GetProvider(queue);
            var persistentQueue = provider.GetJobQueue();

            QueueCommand(persistentQueue.Enqueue(queue, jobId));
        }

        public void IncrementCounter(string key)
        {
            var cmd = new MySqlCommand("insert into Counter (`Key`, `Value`) values (@key, @value)");
            cmd.Parameters.AddWithValue("@key", key);
            cmd.Parameters.AddWithValue("@value", +1);
                
            QueueCommand(cmd);
            
        }

        public void IncrementCounter(string key, TimeSpan expireIn)
        {
            var cmd = new MySqlCommand(
                "insert into Counter (`Key`, `Value`, `ExpireAt`) values (@key, @value, @expireAt)");
            cmd.Parameters.AddWithValue("@key", key);
            cmd.Parameters.AddWithValue("@value", +1);
            cmd.Parameters.AddWithValue("@expireAt", DateTime.UtcNow.Add(expireIn));

            QueueCommand(cmd);
        }

        public void DecrementCounter(string key)
        {
            var cmd = new MySqlCommand("insert into Counter (`Key`, `Value`) values (@key, @value)");
            cmd.Parameters.AddWithValue("@key", key);
            cmd.Parameters.AddWithValue("@value", -1);

            QueueCommand(cmd);
        }

        public void DecrementCounter(string key, TimeSpan expireIn)
        {
            var cmd = 
                new MySqlCommand(
                    "insert into Counter (`Key`, `Value`, `ExpireAt`) values (@key, @value, @expireAt)");
            cmd.Parameters.AddWithValue("@key", key);
            cmd.Parameters.AddWithValue("@value", -1);
            cmd.Parameters.AddWithValue("@expireAt", DateTime.UtcNow.Add(expireIn));

            QueueCommand(cmd);
        }

        public void AddToSet(string key, string value)
        {
            AddToSet(key, value, 0.0);
        }

        public void AddToSet(string key, string value, double score)
        {
            AcquireSetLock();

            var cmd = 
                new MySqlCommand(
                    "INSERT INTO `Set` (`Key`, `Value`, `Score`) " +
                    "VALUES (@Key, @Value, @Score) " +
                    "ON DUPLICATE KEY UPDATE `Score` = @Score");
            cmd.Parameters.AddWithValue("@Key", key);
            cmd.Parameters.AddWithValue("@Value", value);
            cmd.Parameters.AddWithValue("@Score", score);
            QueueCommand(cmd);
        }

        public void RemoveFromSet(string key, string value)
        {
            AcquireSetLock();

            var cmd = new MySqlCommand("delete from `Set` where `Key` = @key and Value = @value");
            cmd.Parameters.AddWithValue("@Key", key);
            cmd.Parameters.AddWithValue("@Value", value);
            QueueCommand(cmd);
        }

        public void InsertToList(string key, string value)
        {
            AcquireListLock();

            var cmd = new MySqlCommand("insert into List (`Key`, Value) values (@key, @value)");
            cmd.Parameters.AddWithValue("@key", key);
            cmd.Parameters.AddWithValue("@value", value);

            QueueCommand(cmd);
        }
        
        public void RemoveFromList(string key, string value)
        {
            AcquireListLock();

            var cmd = new MySqlCommand("delete from List where `Key` = @key and Value = @value");
            cmd.Parameters.AddWithValue("@key", key);
            cmd.Parameters.AddWithValue("@value", value);

            QueueCommand(cmd);
        }

        public void TrimList(string key, int keepStartingFrom, int keepEndingAt)
        {
            AcquireListLock();

            var cmd =
                new MySqlCommand(
@"delete lst
from List lst
	inner join (SELECT tmp.Id, @rownum := @rownum + 1 AS rank
		  		FROM List tmp, 
       				(SELECT @rownum := -1) r ) ranked on ranked.Id = lst.Id
where lst.Key = @key
    and ranked.rank not between @start and @end");
            cmd.Parameters.AddWithValue("@key", key);
            cmd.Parameters.AddWithValue("@start", keepStartingFrom + 1);
            cmd.Parameters.AddWithValue("@end", keepEndingAt + 1);
            QueueCommand(cmd);
        }

        public void SetRangeInHash(string key, IEnumerable<KeyValuePair<string, string>> keyValuePairs)
        {
            if (key == null) throw new ArgumentNullException("key");
            if (keyValuePairs == null) throw new ArgumentNullException("keyValuePairs");

            AcquireHashLock();

            foreach (var y in keyValuePairs)
            {
                var cmd =
                new MySqlCommand(
                    "insert into Hash (`Key`, Field, Value) " +
                    "values (@key, @field, @value) " +
                    "on duplicate key update Value = @value");
                cmd.Parameters.AddWithValue("@key", key);
                cmd.Parameters.AddWithValue("@field", y.Key);
                cmd.Parameters.AddWithValue("@value", y.Value);
                QueueCommand(cmd);
            }
        }
        
        public void RemoveHash(string key)
        {
            if (key == null) throw new ArgumentNullException("key");

            AcquireHashLock();

            var cmd = new MySqlCommand("delete from Hash where `Key` = @key");
            cmd.Parameters.AddWithValue("@key", key);

            QueueCommand(cmd);
        }

        public void Commit()
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
                    command.Connection = connection;
                    command.ExecuteNonQuery();
                }
            });
        }

        internal void QueueCommand(IDbCommand command)
        {
            _commandQueue.Enqueue(command);
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
