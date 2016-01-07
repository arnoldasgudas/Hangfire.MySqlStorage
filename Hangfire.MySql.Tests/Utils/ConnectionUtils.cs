using System;
using MySql.Data.MySqlClient;

namespace Hangfire.MySql.Tests
{
    public static class ConnectionUtils
    {
        private const string DatabaseVariable = "Hangfire_SqlServer_DatabaseName";
        private const string ConnectionStringTemplateVariable 
            = "Hangfire_SqlServer_ConnectionStringTemplate";

        private const string MasterDatabaseName = "mysql";
        private const string DefaultDatabaseName = @"Hangfire.MySql.Tests";
        private const string DefaultConnectionStringTemplate
            = "server=127.0.0.1;uid=root;pwd=root;database={0};Allow User Variables=True";
            
        public static string GetDatabaseName()
        {
            return Environment.GetEnvironmentVariable(DatabaseVariable) ?? DefaultDatabaseName;
        }

        public static string GetMasterConnectionString()
        {
            return String.Format(GetConnectionStringTemplate(), MasterDatabaseName);
        }

        public static string GetConnectionString()
        {
            return String.Format(GetConnectionStringTemplate(), GetDatabaseName());
        }

        private static string GetConnectionStringTemplate()
        {
            return Environment.GetEnvironmentVariable(ConnectionStringTemplateVariable)
                   ?? DefaultConnectionStringTemplate;
        }

        public static MySqlConnection CreateConnection()
        {
            var connection = new MySqlConnection(GetConnectionString());
            connection.Open();

            return connection;
        }
    }
}
