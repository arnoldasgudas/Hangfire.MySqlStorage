# Hangfire MySql Storage Implementation
[![NuGet version](https://badge.fury.io/nu/Hangfire.MySqlStorage.svg)](https://www.nuget.org/packages/Hangfire.MySqlStorage)

> [!IMPORTANT]
> **Maintenance status: not actively maintained**
>
> This library is no longer actively developed. The latest release, [2.0.3](https://www.nuget.org/packages/Hangfire.MySqlStorage/2.0.3), is stable and is still downloaded about 1,400 times a day, but new issues and pull requests are not being reviewed right now.
>
> **If you use it:** pin to version 2.0.3 and test before upgrading Hangfire or MySQL. You're welcome to fork the project under its LGPL-3.0 licence.
>
> **Looking for co-maintainers.** If you rely on this package and would like to help review pull requests and publish releases, please open an issue titled "Co-maintainer" with a short note about your experience with Hangfire and MySQL.
>
> Thank you to everyone who has used, tested and contributed to this project over 5.5 million downloads.

MySql storage implementation of [Hangfire](http://hangfire.io/) - fire-and-forget, delayed and recurring tasks runner for .NET. Scalable and reliable background job runner. Supports multiple servers, CPU and I/O intensive, long-running and short-running jobs.


## Installation
Install MySQL

Run the following command in the NuGet Package Manager console to install Hangfire.MySqlStorage:

```
Install-Package Hangfire.MySqlStorage
```

## Usage

Use one the following ways to initialize `MySqlStorage`: 
- Create new instance of `MySqlStorage` with connection string constructor parameter and pass it to `Configuration` with `UseStorage` method:
```
  GlobalConfiguration.Configuration.UseStorage(
    new MySqlStorage(connectionString));
```
- There must be `Allow User Variables` set to `true` in the connection string. For example: `server=127.0.0.1;uid=root;pwd=root;database={0};Allow User Variables=True`
- Alternatively one or more options can be passed as a parameter to `MySqlStorage`:
```
GlobalConfiguration.Configuration.UseStorage(
    new MySqlStorage(
        connectionString, 
        new MySqlStorageOptions
        {
            TransactionIsolationLevel = IsolationLevel.ReadCommitted,
            QueuePollInterval = TimeSpan.FromSeconds(15),
            JobExpirationCheckInterval = TimeSpan.FromHours(1),
            CountersAggregateInterval = TimeSpan.FromMinutes(5),
            PrepareSchemaIfNecessary = true,
            DashboardJobListLimit = 50000,
            TransactionTimeout = TimeSpan.FromMinutes(1),
            TablesPrefix = "Hangfire"
        }));
```
Description of optional parameters:
- `TransactionIsolationLevel` - transaction isolation level. Default is read committed.
- `QueuePollInterval` - job queue polling interval. Default is 15 seconds.
- `JobExpirationCheckInterval` - job expiration check interval (manages expired records). Default is 1 hour.
- `CountersAggregateInterval` - interval to aggregate counter. Default is 5 minutes.
- `PrepareSchemaIfNecessary` - if set to `true`, it creates database tables. Default is `true`.
- `DashboardJobListLimit` - dashboard job list limit. Default is 50000.
- `TransactionTimeout` - transaction timeout. Default is 1 minute.
- `TablesPrefix` - prefix for the tables in database. Default is none

### How to limit number of open connections

Number of opened connections depends on Hangfire worker count. You can limit worker count by setting `WorkerCount` property value in `BackgroundJobServerOptions`:
```
app.UseHangfireServer(
   new BackgroundJobServerOptions
   {
      WorkerCount = 1
   });
```
More info: http://hangfire.io/features.html#concurrency-level-control

## Dashboard
Hangfire provides a dashboard
![Dashboard](https://camo.githubusercontent.com/f263ab4060a09e4375cc4197fb5bfe2afcacfc20/687474703a2f2f68616e67666972652e696f2f696d672f75692f64617368626f6172642d736d2e706e67)
More info: [Hangfire Overview](http://hangfire.io/overview.html#integrated-monitoring-ui)

## Build
Please use Visual Studio or any other tool of your choice to build the solution, or from the command line (requires the [.NET SDK](https://dotnet.microsoft.com/download)):

```
dotnet build Hangfire.MySqlStorage.sln
```

## Test
The tests include integration tests, so a MySQL server must be running and reachable before you start. By default the tests connect to `127.0.0.1:3306` as `root` / `root`. The test database is created automatically if it does not exist and is dropped again afterwards, so use a throwaway server rather than one holding data you care about.

To use different settings, set the following variables in your system environment variables (restart of Visual Studio is required):

`Hangfire_SqlServer_ConnectionStringTemplate` (default: `server=127.0.0.1;uid=root;pwd=root;database={0};Allow User Variables=True`)

`Hangfire_SqlServer_DatabaseName` (default: `Hangfire.MySql.Tests`)

### Running the tests from the command line

```
dotnet test Hangfire.MySql.Tests
```

The test project targets `netcoreapp2.0`. If you only have a newer .NET runtime installed, let it roll forward:

```
DOTNET_ROLL_FORWARD=Major dotnet test Hangfire.MySql.Tests
```

On Apple Silicon Macs there is no arm64 build of the .NET Core 2.0 runtime, so also pass `--arch arm64`, otherwise the test host looks for an x64 `dotnet`:

```
DOTNET_ROLL_FORWARD=Major dotnet test Hangfire.MySql.Tests --arch arm64
```

## Existing databases

`PrepareSchemaIfNecessary` creates missing tables only. It does not `ALTER` a database that already has the `Job` table. On that path it reads the `{prefix}Schema` version marker (table prefix included, so `TablesPrefix = "Hangfire"` uses `HangfireSchema`) and checks `information_schema`. If a Hangfire table is still `utf8`/`utf8mb3`, or `{prefix}Set.Score` is still `float`, it logs a warning and leaves the data unchanged, including when a version row is already present. When the existing tables already match the current schema but the marker is missing, it records the current version in `{prefix}Schema`. Later startups that already have that version, and that are not still `utf8`/`utf8mb3` or `float`, only log that the tables exist. `PrepareSchemaIfNecessary = false` skips installation and this check.

New installs use `utf8mb4` / `utf8mb4_unicode_520_ci` and store `Set.Score` as `double` (64-bit). MySQL `utf8` is `utf8mb3` and cannot store emoji: a strict `sql_mode` rejects the insert, while a non-strict one silently stores `?` instead (migrating cannot recover data already replaced). `utf8mb4_general_ci` stores emoji but treats every non-BMP character as equal, so `AddToSet` (`ON DUPLICATE KEY UPDATE`) overwrites values that differ only by emoji. `utf8mb4_unicode_520_ci` keeps different emoji distinct and stays case- and accent-insensitive. It does ignore some invisible characters, so values that differ only by a zero-width joiner (U+200D) or a variation selector such as U+FE0F (for example `❤` and `❤️`) compare equal, as do `ß` and `ss`. Avoid keys, set values and ids that differ only that way. MySQL `FLOAT` is 32-bit, so delayed and recurring scores lose precision. String columns are `VARCHAR` so they inherit `utf8mb4`. MySQL `NVARCHAR` is fixed to `utf8mb3`. C# already reads and writes scores as `double`.

Back up the database before migrating. These `ALTER TABLE` statements rebuild each table and block writes until the rebuild finishes, so run them during downtime. The statements are not transactional: if one fails (for example with `Duplicate entry` on `Set` or `Hash` when values that were distinct under `utf8_general_ci` become equal under the new collation, such as `Straße` and `Strasse`), the tables converted before it stay converted. Fix the data and run the whole block again; running it twice is safe.

To upgrade an older schema, run this once. If `TablesPrefix` is set, prefix each table name (for example `HangfireJob`). Widening `Score` keeps stored values but cannot restore precision already lost. The `Set` unique key is 1424 bytes, so the tables need the `DYNAMIC` row format, which is the default on MySQL 5.7 and later. The previous schema already needed it. On MySQL 5.6 that means `innodb_large_prefix=ON`, `innodb_file_format=Barracuda` and adding `ROW_FORMAT=DYNAMIC` to the tables yourself, because `Install.sql` does not set a row format.

```sql
SET FOREIGN_KEY_CHECKS = 0;
ALTER TABLE `Job` CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_520_ci;
ALTER TABLE `Counter` CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_520_ci;
ALTER TABLE `AggregatedCounter` CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_520_ci;
ALTER TABLE `DistributedLock` CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_520_ci;
ALTER TABLE `Hash` CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_520_ci;
ALTER TABLE `JobParameter` CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_520_ci;
ALTER TABLE `JobQueue` CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_520_ci;
ALTER TABLE `JobState` CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_520_ci;
ALTER TABLE `Server` CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_520_ci;
ALTER TABLE `Set` CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_520_ci, MODIFY `Score` double NOT NULL;
ALTER TABLE `State` CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_520_ci;
ALTER TABLE `List` CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_520_ci;
CREATE TABLE IF NOT EXISTS `Schema` (
  `Version` int(11) NOT NULL,
  PRIMARY KEY (`Version`)
) ENGINE=InnoDB DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_520_ci;
INSERT IGNORE INTO `Schema` (`Version`) VALUES (1);
SET FOREIGN_KEY_CHECKS = 1;
```

Version `1` means `utf8mb4` / `utf8mb4_unicode_520_ci` and `Set.Score` stored as `double`. After this script, the next startup finds the marker and does not warn. If you skip the `Schema` statements, a startup with `PrepareSchemaIfNecessary = true` records version `1` itself once the other tables already match.


