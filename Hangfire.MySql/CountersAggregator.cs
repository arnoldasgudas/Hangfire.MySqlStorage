using System;
using System.Threading;
using Dapper;
using Hangfire.Logging;
using Hangfire.Server;

namespace Hangfire.MySql
{
    internal class CountersAggregator : IServerComponent
    {
        private static readonly ILog Logger = LogProvider.GetLogger(typeof(CountersAggregator));

        private const int NumberOfRecordsInSinglePass = 1000;
        private static readonly TimeSpan DelayBetweenPasses = TimeSpan.FromMilliseconds(500);

        private readonly MySqlStorage _storage;
        private readonly MySqlStorageOptions _storageOptions;

        public CountersAggregator(MySqlStorage storage, MySqlStorageOptions storageOptions)
        {
            if (storage == null) throw new ArgumentNullException("storage");

            _storage = storage;
            _storageOptions = storageOptions;
        }

        public void Execute(CancellationToken cancellationToken)
        {
            Logger.DebugFormat($"Aggregating records in '{_storageOptions.TablesPrefix}Counter' table...");

            int removedCount = 0;

            do
            {
                _storage.UseConnection(connection =>
                {
                    removedCount = connection.Execute(
                        GetAggregationQuery(),
                        new { now = DateTime.UtcNow, count = NumberOfRecordsInSinglePass });
                });

                if (removedCount >= NumberOfRecordsInSinglePass)
                {
                    cancellationToken.WaitHandle.WaitOne(DelayBetweenPasses);
                    cancellationToken.ThrowIfCancellationRequested();
                }
            } while (removedCount >= NumberOfRecordsInSinglePass);

            cancellationToken.WaitHandle.WaitOne(_storageOptions.CountersAggregateInterval);
        }

        public override string ToString()
        {
            return GetType().ToString();
        }

        private string GetAggregationQuery()
        {
            return $@"
SET TRANSACTION ISOLATION LEVEL READ COMMITTED;
START TRANSACTION;

INSERT INTO `{_storageOptions.TablesPrefix}AggregatedCounter` (`Key`, Value, ExpireAt)
    SELECT `Key`, SUM(Value) as Value, MAX(ExpireAt) AS ExpireAt 
    FROM (
            SELECT `Key`, Value, ExpireAt
            FROM `{_storageOptions.TablesPrefix}Counter`
            LIMIT @count) tmp
	GROUP BY `Key`
        ON DUPLICATE KEY UPDATE 
            Value = Value + VALUES(Value),
            ExpireAt = GREATEST(ExpireAt,VALUES(ExpireAt));

DELETE FROM `{_storageOptions.TablesPrefix}Counter`
LIMIT @count;

COMMIT;";
        }
    }
}
