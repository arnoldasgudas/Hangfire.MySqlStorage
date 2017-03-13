using System;
using System.IO;
using System.Reflection;
using Dapper;
using Hangfire.Logging;
using MySql.Data.MySqlClient;

namespace Hangfire.MySql
{
    public static class MySqlObjectsInstaller
    {
        private static readonly ILog Log = LogProvider.GetLogger(typeof(MySqlStorage));
        public static void Install(MySqlConnection connection)
        {
            if (connection == null) throw new ArgumentNullException("connection");

            if (TablesExists(connection))
            {
                Log.Info("DB tables already exist. Exit install");
                return;
            }

            Log.Info("Start installing Hangfire SQL objects...");

            var script = GetStringResource("Hangfire.MySql.Install.sql");

            connection.Execute(script);

            Log.Info("Hangfire SQL objects installed.");
        }

        private static bool TablesExists(MySqlConnection connection)
        {
            return connection.ExecuteScalar<string>("SHOW TABLES LIKE 'Job';") != null;            
        }

        private static string GetStringResource(string resourceName)
        {
#if NET45
            var assembly = typeof(MySqlObjectsInstaller).Assembly;
#else
            var assembly = typeof(MySqlObjectsInstaller).GetTypeInfo().Assembly;
#endif

            using (var stream = assembly.GetManifestResourceStream(resourceName))
            {
                if (stream == null)
                {
                    throw new InvalidOperationException(String.Format(
                        "Requested resource `{0}` was not found in the assembly `{1}`.",
                        resourceName,
                        assembly));
                }

                using (var reader = new StreamReader(stream))
                {
                    return reader.ReadToEnd();
                }
            }
        }
    }
}