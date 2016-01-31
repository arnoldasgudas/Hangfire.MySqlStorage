using System;
using System.Threading;
using Dapper;
using MySql.Data.MySqlClient;

namespace Hangfire.MySql.Tests
{
    public class TestDatabaseFixture : IDisposable
    {
        private static readonly object GlobalLock = new object();
        public TestDatabaseFixture()
        {
            Monitor.Enter(GlobalLock);
            CreateAndInitializeDatabase();
        }
        public void Dispose()
        {
            DropDatabase();
            Monitor.Exit(GlobalLock);
        }

        private static void CreateAndInitializeDatabase()
        {
            var recreateDatabaseSql = String.Format(
                @"CREATE DATABASE IF NOT EXISTS `{0}`",
                ConnectionUtils.GetDatabaseName());

            using (var connection = new MySqlConnection(
                ConnectionUtils.GetMasterConnectionString()))
            {
                connection.Execute(recreateDatabaseSql);
            }

            using (var connection = new MySqlConnection(
                ConnectionUtils.GetConnectionString()))
            {
                MySqlObjectsInstaller.Install(connection);
            }
        }

        private static void DropDatabase()
        {
            var recreateDatabaseSql = String.Format(
                   @"DROP DATABASE IF EXISTS `{0}`",
                   ConnectionUtils.GetDatabaseName());

            using (var connection = new MySqlConnection(
                ConnectionUtils.GetMasterConnectionString()))
            {
                connection.Execute(recreateDatabaseSql);
            }
        }
    }
}
