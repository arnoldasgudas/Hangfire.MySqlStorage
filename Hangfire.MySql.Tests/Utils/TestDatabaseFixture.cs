using System;
using Dapper;
using MySql.Data.MySqlClient;

namespace Hangfire.MySql.Tests
{
    public class TestDatabaseFixture : IDisposable
    {
        public TestDatabaseFixture()
        {
            CreateAndInitializeDatabase();
        }
        public void Dispose()
        {
            DropDatabase();
        }

        private static void CreateAndInitializeDatabase()
        {
            var recreateDatabaseSql = String.Format(
                @"DROP DATABASE IF EXISTS `{0}`; CREATE DATABASE `{0}`",
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
