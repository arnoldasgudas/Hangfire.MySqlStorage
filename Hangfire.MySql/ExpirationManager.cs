using System;
using System.Threading;
using Dapper;
using Hangfire.Logging;
using Hangfire.Server;
using MySql.Data.MySqlClient;

namespace Hangfire.MySql
{
    internal class ExpirationManager : IServerComponent
    {
        private static readonly ILog Logger = LogProvider.GetLogger(typeof(ExpirationManager));

        private static readonly TimeSpan DefaultLockTimeout = TimeSpan.FromSeconds(30);
        private const string DistributedLockKey = "expirationmanager";
        private static readonly TimeSpan DelayBetweenPasses = TimeSpan.FromSeconds(1);
        private const int NumberOfRecordsInSinglePass = 1000;

        private readonly string[] _processedTables;

        private readonly MySqlStorage _storage;
        private readonly MySqlStorageOptions _storageOptions;

        public ExpirationManager(MySqlStorage storage, MySqlStorageOptions storageOptions)
        {
            _storage = storage ?? throw new ArgumentNullException("storage");
            _storageOptions = storageOptions ?? throw new ArgumentNullException(nameof(storageOptions));

            _processedTables = new[]
            {
                $"{storageOptions.TablesPrefix}AggregatedCounter",
                $"{storageOptions.TablesPrefix}Job",
                $"{storageOptions.TablesPrefix}List",
                $"{storageOptions.TablesPrefix}Set",
                $"{storageOptions.TablesPrefix}Hash",
            };
        }

        public void Execute(CancellationToken cancellationToken)
        {
            foreach (var table in _processedTables)
            {
                Logger.DebugFormat("Removing outdated records from table '{0}'...", table);

                int removedCount = 0;

                do
                {
                    _storage.UseConnection(connection =>
                    {
                        try
                        {
                            Logger.DebugFormat("delete from `{0}` where ExpireAt < @now limit @count;", table);

                            using (
                                new MySqlDistributedLock(
                                    connection,
                                    DistributedLockKey,
                                    DefaultLockTimeout,
                                    _storageOptions,
                                    cancellationToken).Acquire())
                            {
                                removedCount = connection.Execute(
                                    String.Format(
                                        "select null from `{0}` where ExpireAt < @now; " +
                                        "delete from `{0}` where ExpireAt < @now limit @count;", table),
                                    new { now = DateTime.UtcNow, count = NumberOfRecordsInSinglePass });
                            }

                            Logger.DebugFormat("removed records count={0}", removedCount);
                        }
                        catch (MySqlException ex)
                        {
                            Logger.Error(ex.ToString());
                        }
                    });

                    if (removedCount > 0)
                    {
                        Logger.Trace(String.Format("Removed {0} outdated record(s) from '{1}' table.", removedCount,
                            table));

                        cancellationToken.WaitHandle.WaitOne(DelayBetweenPasses);
                        cancellationToken.ThrowIfCancellationRequested();
                    }
                } while (removedCount > 0);
            }

            cancellationToken.WaitHandle.WaitOne(_storageOptions.JobExpirationCheckInterval);
        }

        public override string ToString()
        {
            return GetType().ToString();
        }
    }
}
