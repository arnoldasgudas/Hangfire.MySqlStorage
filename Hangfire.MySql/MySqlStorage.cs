using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Transactions;
using Hangfire.Annotations;
using Hangfire.MySql.JobQueue;
using Hangfire.Storage;
using MySql.Data.MySqlClient;

namespace Hangfire.MySql
{
    public class MySqlStorage : JobStorage, IDisposable
    {
        private readonly string _connectionString;
        private readonly MySqlStorageOptions _options;
        private readonly HashSet<MySqlConnection> _connections = new HashSet<MySqlConnection>(); 

        public virtual PersistentJobQueueProviderCollection QueueProviders { get; private set; }

        public MySqlStorage(string nameOrConnectionString)
            : this(nameOrConnectionString, new MySqlStorageOptions())
        {
        }

        public MySqlStorage(string nameOrConnectionString, MySqlStorageOptions options)
        {
            if (nameOrConnectionString == null) throw new ArgumentNullException("nameOrConnectionString");
            if (options == null) throw new ArgumentNullException("options");

            if (IsConnectionString(nameOrConnectionString))
            {
                _connectionString = nameOrConnectionString;
            }
            else if (IsConnectionStringInConfiguration(nameOrConnectionString))
            {
                _connectionString = ConfigurationManager.ConnectionStrings[nameOrConnectionString].ConnectionString;
            }
            else
            {
                throw new ArgumentException(
                    string.Format(
                        "Could not find connection string with name '{0}' in application config file",
                        nameOrConnectionString));
            }
            _options = options;

            InitializeQueueProviders();
        }

        private void InitializeQueueProviders()
        {
            QueueProviders = 
                new PersistentJobQueueProviderCollection(
                    new MySqlJobQueueProvider(this, _options));
        }

        public override IMonitoringApi GetMonitoringApi()
        {
            throw new System.NotImplementedException();
        }

        public override IStorageConnection GetConnection()
        {
            return new MySqlStorageConnection(this);
        }

        private bool IsConnectionString(string nameOrConnectionString)
        {
            return nameOrConnectionString.Contains(";");
        }

        private bool IsConnectionStringInConfiguration(string connectionStringName)
        {
            var connectionStringSetting = ConfigurationManager.ConnectionStrings[connectionStringName];

            return connectionStringSetting != null;
        }

        internal void UseTransaction([InstantHandle] Action<MySqlConnection> action)
        {
            UseTransaction(connection =>
            {
                action(connection);
                return true;
            }, null);
        }

        internal T UseTransaction<T>(
            [InstantHandle] Func<MySqlConnection, T> func, IsolationLevel? isolationLevel)
        {
            using (var transaction = CreateTransaction(isolationLevel ?? _options.TransactionIsolationLevel))
            {
                var result = UseConnection(func);
                transaction.Complete();

                return result;
            }
        }
        private TransactionScope CreateTransaction(IsolationLevel? isolationLevel)
        {
            return isolationLevel != null
                ? new TransactionScope(TransactionScopeOption.Required,
                    new TransactionOptions { IsolationLevel = isolationLevel.Value, Timeout = _options.TransactionTimeout })
                : new TransactionScope();
        }

        internal void UseConnection([InstantHandle] Action<MySqlConnection> action)
        {
            UseConnection(connection =>
            {
                action(connection);
                return true;
            });
        }

        internal T UseConnection<T>([InstantHandle] Func<MySqlConnection, T> func)
        {
            MySqlConnection connection = null;

            try
            {
                connection = CreateAndOpenConnection();
                return func(connection);
            }
            finally
            {
                ReleaseConnection(connection);
            }
        }

        internal MySqlConnection CreateAndOpenConnection()
        {
            var connection = new MySqlConnection(_connectionString);
            connection.Open();
            _connections.Add(connection);
            return connection;
        }

        internal void ReleaseConnection(MySqlConnection connection)
        {
            connection.Dispose();
            _connections.Remove(connection);
        }
        public void Dispose()
        {
            _connections.ToList().ForEach(connection => connection.Dispose());
            _connections.Clear();
        }
    }
}
