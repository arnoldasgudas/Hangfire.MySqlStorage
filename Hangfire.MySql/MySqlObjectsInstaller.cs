using System;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using Dapper;
using Hangfire.Logging;
using MySqlConnector;

namespace Hangfire.MySql
{
    public static class MySqlObjectsInstaller
    {
        // Version 1: utf8mb4 / utf8mb4_unicode_520_ci and Set.Score as double (#106),
        // plus this schema marker. Bump this together with MatchesCurrentShape.
        public const int LatestSchemaVersion = 1;

        private static readonly string[] KnownTables =
        {
            "AggregatedCounter",
            "Counter",
            "DistributedLock",
            "Hash",
            "Job",
            "JobParameter",
            "JobQueue",
            "JobState",
            "List",
            "Server",
            "Set",
            "State"
        };

        public static void Install(MySqlConnection connection, string tablesPrefix = null)
        {
            if (connection == null) throw new ArgumentNullException("connection");

            var prefix = tablesPrefix ?? string.Empty;
            var log = LogProvider.GetLogger(typeof(MySqlStorage));
            if (!TablesExists(connection, prefix))
            {
                log.Info("Start installing Hangfire SQL objects...");

                var script = GetStringResource("Hangfire.MySql.Install.sql");
                var formattedScript = GetFormattedScript(script, prefix);

                connection.Execute(formattedScript);

                log.Info("Hangfire SQL objects installed.");
                return;
            }

            var version = GetSchemaVersion(connection, prefix);
            var inspection = InspectSchema(connection, prefix);

            if (version.HasValue
                && version.Value >= LatestSchemaVersion
                && !inspection.ScoreIsFloat
                && inspection.LegacyCharsetTables.Length == 0)
            {
                log.Info("DB tables already exist. Exit install");
                return;
            }

            if (inspection.MatchesCurrentShape)
            {
                EnsureSchemaMarker(connection, prefix, LatestSchemaVersion);
                log.Info("DB tables already exist and match schema version " + LatestSchemaVersion + ". Recorded schema version.");
                return;
            }

            if (inspection.ScoreIsFloat || inspection.LegacyCharsetTables.Length > 0)
            {
                log.Warn(BuildLegacyWarning(prefix, version, inspection));
                return;
            }

            log.Info("DB tables already exist. Exit install");
        }

        private static bool TablesExists(MySqlConnection connection, string tablesPrefix)
        {
            return TableExists(connection, tablesPrefix + "Job");
        }

        private static bool TableExists(MySqlConnection connection, string tableName)
        {
            return connection.ExecuteScalar<int>(
                       "select count(*) from information_schema.tables " +
                       "where table_schema = database() and table_name = @tableName",
                       new { tableName }) > 0;
        }

        private static int? GetSchemaVersion(MySqlConnection connection, string tablesPrefix)
        {
            var tableName = tablesPrefix + "Schema";
            if (!TableExists(connection, tableName))
            {
                return null;
            }

            return connection.ExecuteScalar<int?>(
                "select max(`Version`) from " + QuoteIdentifier(tableName));
        }

        private static void EnsureSchemaMarker(MySqlConnection connection, string tablesPrefix, int version)
        {
            var table = QuoteIdentifier(tablesPrefix + "Schema");
            connection.Execute(
                "CREATE TABLE IF NOT EXISTS " + table + " (" +
                "`Version` int(11) NOT NULL, " +
                "PRIMARY KEY (`Version`)" +
                ") ENGINE=InnoDB DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_520_ci");

            connection.Execute(
                "INSERT IGNORE INTO " + table + " (`Version`) VALUES (@version)",
                new { version });
        }

        private static SchemaInspection InspectSchema(MySqlConnection connection, string tablesPrefix)
        {
            var names = new string[KnownTables.Length];
            for (var i = 0; i < KnownTables.Length; i++)
            {
                names[i] = tablesPrefix + KnownTables[i];
            }

            var existingCount = connection.ExecuteScalar<int>(
                "select count(*) from information_schema.tables " +
                "where table_schema = database() and table_name in @names",
                new { names });

            var legacyTables = connection.Query<string>(
                "select distinct table_name from information_schema.columns " +
                "where table_schema = database() and table_name in @names " +
                "and character_set_name in ('utf8', 'utf8mb3')",
                new { names }).ToArray();

            var charsets = connection.Query<string>(
                "select distinct character_set_name from information_schema.columns " +
                "where table_schema = database() and table_name in @names " +
                "and character_set_name is not null",
                new { names }).ToArray();

            var scoreType = connection.ExecuteScalar<string>(
                "select data_type from information_schema.columns " +
                "where table_schema = database() and table_name = @tableName " +
                "and column_name = 'Score'",
                new { tableName = tablesPrefix + "Set" });

            var scoreIsFloat = scoreType != null
                               && scoreType.Equals("float", StringComparison.OrdinalIgnoreCase);
            var scoreIsDouble = scoreType != null
                                && scoreType.Equals("double", StringComparison.OrdinalIgnoreCase);
            var charsetIsUtf8Mb4 = charsets.Length == 1
                                   && charsets[0].Equals("utf8mb4", StringComparison.OrdinalIgnoreCase);

            Array.Sort(legacyTables, StringComparer.Ordinal);

            return new SchemaInspection
            {
                LegacyCharsetTables = legacyTables,
                ScoreIsFloat = scoreIsFloat,
                MatchesCurrentShape = existingCount == names.Length
                                      && charsetIsUtf8Mb4
                                      && scoreIsDouble
                                      && legacyTables.Length == 0
            };
        }

        private static string BuildLegacyWarning(string tablesPrefix, int? version, SchemaInspection inspection)
        {
            var prefixText = tablesPrefix.Length == 0 ? "(none)" : tablesPrefix;
            var details = new StringBuilder();
            if (inspection.LegacyCharsetTables.Length > 0)
            {
                details.Append("tables still using utf8/utf8mb3: ");
                details.Append(string.Join(", ", inspection.LegacyCharsetTables));
            }

            if (inspection.ScoreIsFloat)
            {
                if (details.Length > 0) details.Append("; ");
                details.Append("`");
                details.Append(tablesPrefix);
                details.Append("Set`.`Score` is still float");
            }

            var versionText = version.HasValue
                ? version.Value.ToString(CultureInfo.InvariantCulture)
                : "missing";

            return "Hangfire MySQL schema is not at version " + LatestSchemaVersion
                   + " (table prefix " + prefixText + ", recorded schema version " + versionText + "). "
                   + details
                   + ". The installer does not alter existing tables. "
                   + "Back up the database and run the ALTER statements in the README, then restart. "
                   + "New installs use utf8mb4 / utf8mb4_unicode_520_ci and store Set.Score as double.";
        }

        private static string QuoteIdentifier(string name)
        {
            return "`" + (name ?? string.Empty).Replace("`", "``") + "`";
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

        private static string GetFormattedScript(string script, string tablesPrefix)
        {
            var sb = new StringBuilder(script);
            sb.Replace("[schemaVersion]", LatestSchemaVersion.ToString(CultureInfo.InvariantCulture));
            sb.Replace("[tablesPrefix]", tablesPrefix);

            return sb.ToString();
        }

        private sealed class SchemaInspection
        {
            public string[] LegacyCharsetTables { get; set; }
            public bool ScoreIsFloat { get; set; }
            public bool MatchesCurrentShape { get; set; }
        }
    }
}
