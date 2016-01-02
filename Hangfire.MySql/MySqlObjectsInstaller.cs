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

            Log.Info("Start installing Hangfire SQL objects...");

            var script = GetStringResource(
                typeof(MySqlObjectsInstaller).Assembly,
                "Hangfire.MySql.Install.sql");

            connection.Execute(script);

            Log.Info("Hangfire SQL objects installed.");
        }

        private static string GetStringResource(Assembly assembly, string resourceName)
        {
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