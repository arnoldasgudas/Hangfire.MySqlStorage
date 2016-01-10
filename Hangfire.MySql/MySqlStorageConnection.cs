using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using Dapper;
using Hangfire.Common;
using Hangfire.Logging;
using Hangfire.MySql.Entities;
using Hangfire.Server;
using Hangfire.Storage;

namespace Hangfire.MySql
{
    public class MySqlStorageConnection : JobStorageConnection
    {
        private static readonly ILog Logger = LogProvider.GetCurrentClassLogger();

        private readonly MySqlStorage _storage;
        public MySqlStorageConnection(MySqlStorage storage)
        {
            if (storage == null) throw new ArgumentNullException("storage");
            _storage = storage;
        }

        public override IWriteOnlyTransaction CreateWriteTransaction()
        {
            return new MySqlWriteOnlyTransaction(_storage);
        }

        public override IDisposable AcquireDistributedLock(string resource, TimeSpan timeout)
        {
            return new MySqlDistributedLock(_storage, resource, timeout);
        }

        public override string CreateExpiredJob(Job job, IDictionary<string, string> parameters, DateTime createdAt, TimeSpan expireIn)
        {
            if (job == null) throw new ArgumentNullException("job");
            if (parameters == null) throw new ArgumentNullException("parameters");

            var invocationData = InvocationData.Serialize(job);

            Logger.TraceFormat("CreateExpiredJob={0}", JobHelper.ToJson(invocationData));

            return _storage.UseConnection(connection =>
            {
                var cmd = connection.CreateCommand();
                cmd.CommandText =
                    "insert into Job (InvocationData, Arguments, CreatedAt, ExpireAt) " +
                    "values (@invocationData, @arguments, @createdAt, @expireAt); " +
                    "select last_insert_id();";
                cmd.Parameters.AddWithValue("@invocationData", JobHelper.ToJson(invocationData));
                cmd.Parameters.AddWithValue("@arguments", invocationData.Arguments);
                cmd.Parameters.AddWithValue("@createdAt", createdAt);
                cmd.Parameters.AddWithValue("@expireAt", createdAt.Add(expireIn));
                var jobId = Convert.ToString(cmd.ExecuteScalar());

                if (parameters.Count > 0)
                {
                    var insertParameterSql =
                        new StringBuilder("insert into JobParameter (JobId, Name, Value) values ");

                    var parameterIndex = 0;
                    foreach (var parameter in parameters)
                    {
                        if (parameterIndex > 0)
                        {
                            insertParameterSql.Append(",");
                        }

                        insertParameterSql.AppendFormat(" (@jobId{0}, @name{0}, @value{0}) ", parameterIndex);
                        cmd.Parameters.AddWithValue("@jobId" + parameterIndex, jobId);
                        cmd.Parameters.AddWithValue("@name" + parameterIndex, parameter.Key);
                        cmd.Parameters.AddWithValue("@value" + parameterIndex, parameter.Value);
                        parameterIndex++;
                    }
                    cmd.CommandText = insertParameterSql.ToString();
                    cmd.ExecuteNonQuery();
                }
                return jobId;
            });
        }

        public override IFetchedJob FetchNextJob(string[] queues, CancellationToken cancellationToken)
        {
            if (queues == null || queues.Length == 0) throw new ArgumentNullException("queues");

            var providers = queues
                .Select(queue => _storage.QueueProviders.GetProvider(queue))
                .Distinct()
                .ToArray();

            if (providers.Length != 1)
            {
                throw new InvalidOperationException(String.Format(
                    "Multiple provider instances registered for queues: {0}. You should choose only one type of persistent queues per server instance.",
                    String.Join(", ", queues)));
            }

            var persistentQueue = providers[0].GetJobQueue();
            return persistentQueue.Dequeue(queues, cancellationToken);
        }

        public override void SetJobParameter(string id, string name, string value)
        {
            if (id == null) throw new ArgumentNullException("id");
            if (name == null) throw new ArgumentNullException("name");

            _storage.UseConnection(connection =>
            {
                var cmd = connection.CreateCommand();
                cmd.CommandText =
                    "insert into JobParameter (JobId, Name, Value) " +
                    "value (@jobId, @name, @value) " +
                    "on duplicate key update Value = @value ";
                cmd.Parameters.AddWithValue("@jobId", id);
                cmd.Parameters.AddWithValue("@name", name);
                cmd.Parameters.AddWithValue("@value", value);
                cmd.ExecuteNonQuery(); 
            });
        }

        public override string GetJobParameter(string id, string name)
        {
            if (id == null) throw new ArgumentNullException("id");
            if (name == null) throw new ArgumentNullException("name");

            string result = null;

            return _storage.UseConnection(connection =>
            {
                var cmd = connection.CreateCommand();
                cmd.CommandText =
                    "select Value " +
                    "from JobParameter " +
                    "where JobId = @id and Name = @name";
                cmd.Parameters.AddWithValue("@id", id);
                cmd.Parameters.AddWithValue("@name", name);
                using (var reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        result = reader.GetString("Value");
                    }
                }
                return result;
            });
        }

        public override JobData GetJobData(string jobId)
        {
            if (jobId == null) throw new ArgumentNullException("jobId");

            string invocationDataString = null;
            string arguments = null;
            DateTime createdAt = DateTime.MinValue;
            string state = null;

            return _storage.UseConnection(connection =>
            {
                var cmd = connection.CreateCommand();
                cmd.CommandText =
                    "select InvocationData, StateName, Arguments, CreatedAt " +
                    "from Job " +
                    "where Id = @id";
                cmd.Parameters.AddWithValue("@id", jobId);
                using (var reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        invocationDataString = reader.GetString("InvocationData");
                        arguments = reader.GetString("Arguments");
                        createdAt = reader.GetDateTime("CreatedAt");
                        state = reader.GetString("StateName");
                    }
                }

                if (string.IsNullOrEmpty(invocationDataString)) return null;

                var invocationData = JobHelper.FromJson<InvocationData>(invocationDataString);
                invocationData.Arguments = arguments;

                Job job = null;
                JobLoadException loadException = null;

                try
                {
                    job = invocationData.Deserialize();
                }
                catch (JobLoadException ex)
                {
                    loadException = ex;
                }

                return new JobData
                {
                    Job = job,
                    State = state,
                    CreatedAt = createdAt,
                    LoadException = loadException
                };
            });
        }

        public override StateData GetStateData(string jobId)
        {
            if (jobId == null) throw new ArgumentNullException("jobId");

            SqlState sqlState = null;

            return _storage.UseConnection(connection =>
            {
                var cmd = connection.CreateCommand();
                cmd.CommandText =
                    "select s.Name, s.Reason, s.Data " +
                    "from State s inner join Job j on j.StateId = s.Id " +
                    "where j.Id = @jobId";
                cmd.Parameters.AddWithValue("@jobId", jobId);
                using (var reader = cmd.ExecuteReader())
                {
                    if (reader.Read())
                    {
                        sqlState =
                            new SqlState
                            {
                                Name = reader.GetString("Name"),
                                Reason = reader.GetString("Reason"),
                                Data = reader.GetString("Data")
                            };
                    }
                }
                return
                    sqlState == null
                    ? null
                    : new StateData
                    {
                        Name = sqlState.Name,
                        Reason = sqlState.Reason,
                        Data =
                            new Dictionary<string, string>(
                                JobHelper.FromJson<Dictionary<string, string>>(sqlState.Data),
                                StringComparer.OrdinalIgnoreCase)
                    };
            });
        }

        public override void AnnounceServer(string serverId, ServerContext context)
        {
            if (serverId == null) throw new ArgumentNullException("serverId");
            if (context == null) throw new ArgumentNullException("context");

            _storage.UseConnection(connection =>
            {
                var cmd = connection.CreateCommand();
                cmd.CommandText =
                    "INSERT INTO Server (Id, Data, LastHeartbeat) " +
                    "VALUE (@Id, @Data, @Heartbeat) " +
                    "ON DUPLICATE KEY UPDATE Data = @Data, LastHeartbeat = @Heartbeat";
                cmd.Parameters.AddWithValue("@Id", serverId);
                cmd.Parameters.AddWithValue(
                    "@Data", 
                    JobHelper.ToJson(
                        new ServerData
                        {
                            WorkerCount = context.WorkerCount,
                            Queues = context.Queues,
                            StartedAt = DateTime.UtcNow,
                        }));
                cmd.Parameters.AddWithValue("@Heartbeat", DateTime.Now);
                cmd.ExecuteNonQuery();
            });
        }

        public override void RemoveServer(string serverId)
        {
            if (serverId == null) throw new ArgumentNullException("serverId");

            _storage.UseConnection(connection =>
            {
                var cmd = connection.CreateCommand();
                cmd.CommandText = "delete from Server where Id = @id";
                cmd.Parameters.AddWithValue("@id", serverId);
                cmd.ExecuteNonQuery();
            });
        }

        public override void Heartbeat(string serverId)
        {
            if (serverId == null) throw new ArgumentNullException("serverId");

            _storage.UseConnection(connection =>
            {
                var cmd = connection.CreateCommand();
                cmd.CommandText = "update Server set LastHeartbeat = @now where Id = @id";
                cmd.Parameters.AddWithValue("@id", serverId);
                cmd.Parameters.AddWithValue("@now", DateTime.UtcNow);
                cmd.ExecuteNonQuery();
            });
        }

        public override int RemoveTimedOutServers(TimeSpan timeOut)
        {
            if (timeOut.Duration() != timeOut)
            {
                throw new ArgumentException("The `timeOut` value must be positive.", "timeOut");
            }

            return _storage.UseConnection(connection =>
            {
                var cmd = connection.CreateCommand();
                cmd.CommandText = "delete from Server where LastHeartbeat < @timeOutAt";
                cmd.Parameters.AddWithValue("@timeOutAt", DateTime.UtcNow.Add(timeOut.Negate()));
                return cmd.ExecuteNonQuery();
            });
        }

        public override long GetSetCount(string key)
        {
            if (key == null) throw new ArgumentNullException("key");

            return _storage.UseConnection(connection =>
            {
                var cmd = connection.CreateCommand();
                cmd.CommandText = "select count(`Key`) from `Set` where `Key` = @key";
                cmd.Parameters.AddWithValue("@key", key);
                return (long)cmd.ExecuteScalar();
            });
        }

        public override List<string> GetRangeFromSet(string key, int startingFrom, int endingAt)
        {
            if (key == null) throw new ArgumentNullException("key");

            var result = new List<string>();
            return _storage.UseConnection(connection =>
            {
                var cmd = connection.CreateCommand();
                cmd.CommandText = @"
select `Value` 
from `Set` s 
    inner join (
	    select tmp.Id, @rownum := @rownum + 1 AS rank
	    from `Set` tmp,
            (select @rownum := -1) r ) ranked on ranked.Id = s.Id
where s.`Key` = @key 
    and  ranked.rank between @startingFrom and @endingAt";
                cmd.Parameters.AddWithValue("@key", key);
                cmd.Parameters.AddWithValue("@startingFrom", startingFrom);
                cmd.Parameters.AddWithValue("@endingAt", endingAt);
                using (var reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        result.Add(reader.GetString("Value"));
                    }
                }

                return result;
            });
        }

        public override HashSet<string> GetAllItemsFromSet(string key)
        {
            if (key == null) throw new ArgumentNullException("key");

            var result = new HashSet<string>();
            return _storage.UseConnection(connection =>
            {
                var cmd = connection.CreateCommand();
                cmd.CommandText = "select Value from `Set` where `Key` = @key";
                cmd.Parameters.AddWithValue("@key", key);
                using (var reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        result.Add(reader.GetString("Value"));
                    }
                }

                return result;
            });
        }

        public override string GetFirstByLowestScoreFromSet(string key, double fromScore, double toScore)
        {
            if (key == null) throw new ArgumentNullException("key");
            if (toScore < fromScore) 
                throw new ArgumentException("The `toScore` value must be higher or equal to the `fromScore` value.");

            return _storage.UseConnection(connection =>
            {
                var cmd = connection.CreateCommand();
                cmd.CommandText =
                    "select Value " +
                    "from `Set` " +
                    "where `Key` = @key and Score between @from and @to " +
                    "order by Score " +
                    "limit 1";
                cmd.Parameters.AddWithValue("@key", key);
                cmd.Parameters.AddWithValue("@from", fromScore);
                cmd.Parameters.AddWithValue("@to", toScore);

                using (var reader = cmd.ExecuteReader())
                {
                    if (reader.Read())
                    {
                        return reader.GetString("Value");
                    }
                }
                return null;
            });
        }

        public override long GetCounter(string key)
        {
            if (key == null) throw new ArgumentNullException("key");

            string query = @"
select sum(s.`Value`) from (select sum(`Value`) as `Value` from Counter
where `Key` = @key
union all
select `Value` from AggregatedCounter
where `Key` = @key) as s";

            return 
                _storage
                    .UseConnection(connection =>
                        connection.Query<long?>(query, new { key = key }).Single() ?? 0);
        }

        public override long GetHashCount(string key)
        {
            if (key == null) throw new ArgumentNullException("key");

            return 
                _storage
                    .UseConnection(connection => 
                        connection.Query<long>(
                            "select count(Id) from Hash where `Key` = @key", 
                            new { key = key }).Single());
        }

        public override TimeSpan GetHashTtl(string key)
        {
            if (key == null) throw new ArgumentNullException("key");

            return _storage.UseConnection(connection =>
            {
                var result = 
                    connection.Query<DateTime?>(
                        "select min(ExpireAt) from Hash where `Key` = @key", 
                        new { key = key }).Single();
                if (!result.HasValue) return TimeSpan.FromSeconds(-1);

                return result.Value - DateTime.UtcNow;
            });
        }

        public override long GetListCount(string key)
        {
            if (key == null) throw new ArgumentNullException("key");

            return 
                _storage
                    .UseConnection(connection => 
                        connection.Query<long>(
                            "select count(Id) from List where `Key` = @key", 
                            new { key = key }).Single());
        }

        public override TimeSpan GetListTtl(string key)
        {
            if (key == null) throw new ArgumentNullException("key");

            return _storage.UseConnection(connection =>
            {
                var result = 
                    connection.Query<DateTime?>(
                        "select min(ExpireAt) from List where `Key` = @key", 
                        new { key = key }).Single();
                if (!result.HasValue) return TimeSpan.FromSeconds(-1);

                return result.Value - DateTime.UtcNow;
            });
        }

        public override string GetValueFromHash(string key, string name)
        {
            if (key == null) throw new ArgumentNullException("key");
            if (name == null) throw new ArgumentNullException("name");

            return 
                _storage
                    .UseConnection(connection => 
                        connection.Query<string>(
                            "select `Value` from Hash where `Key` = @key and `Field` = @field", 
                            new { key = key, field = name }).SingleOrDefault());
        }

        public override List<string> GetRangeFromList(string key, int startingFrom, int endingAt)
        {
            if (key == null) throw new ArgumentNullException("key");

            string query = @"
select `Value` 
from List lst
    inner join (
        select tmp.Id, @rownum := @rownum + 1 AS rank
	    from `List` tmp,
            (select @rownum := -1) r 
        ) ranked on ranked.Id = lst.Id
where lst.`Key` = @key 
    and  ranked.rank between @startingFrom and @endingAt
order by lst.Id desc";

            return
                _storage
                    .UseConnection(connection =>
                        connection.Query<string>(
                            query,
                            new {key = key, startingFrom = startingFrom + 1, endingAt = endingAt + 1})
                            .ToList());
        }

        public override List<string> GetAllItemsFromList(string key)
        {
            if (key == null) throw new ArgumentNullException("key");

            string query = @"
select `Value` from List
where `Key` = @key
order by Id desc";

            return _storage.UseConnection(connection => connection.Query<string>(query, new { key = key }).ToList());
        }

        public override TimeSpan GetSetTtl(string key)
        {
            if (key == null) throw new ArgumentNullException("key");

            return _storage.UseConnection(connection =>
            {
                var result = 
                    connection
                        .Query<DateTime?>(
                            "select min(ExpireAt) from `Set` where `Key` = @key", 
                            new { key = key }).Single();
                if (!result.HasValue) return TimeSpan.FromSeconds(-1);

                return result.Value - DateTime.UtcNow;
            });
        }

        public override void SetRangeInHash(string key, IEnumerable<KeyValuePair<string, string>> keyValuePairs)
        {
            if (key == null) throw new ArgumentNullException("key");
            if (keyValuePairs == null) throw new ArgumentNullException("keyValuePairs");

            _storage.UseConnection(connection =>
            {
                foreach (var keyValuePair in keyValuePairs)
                {
                    var cmd = connection.CreateCommand();
                    cmd.CommandText =
                        "insert into Hash (`Key`, Field, Value) " +
                        "value (@key, @field, @value) " +
                        "on duplicate key update Value = @value";
                    cmd.Parameters.AddWithValue("@key", key);
                    cmd.Parameters.AddWithValue("@field", keyValuePair.Key);
                    cmd.Parameters.AddWithValue("@value", keyValuePair.Value);
                    cmd.ExecuteNonQuery();
                }
            });
        }

        public override Dictionary<string, string> GetAllEntriesFromHash(string key)
        {
            if (key == null) throw new ArgumentNullException("key");

            var result = new Dictionary<string, string>();

            return _storage.UseConnection(connection =>
            {
                var cmd = connection.CreateCommand();
                cmd.CommandText = "select Field, Value from Hash where `Key` = @key";
                cmd.Parameters.AddWithValue("@key", key);
                using (var reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        result.Add(reader.GetString("Field"), reader.GetString("Value"));
                    }
                }

                return result.Count != 0 ? result : null;
            });
        }
    }
}