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

`PrepareSchemaIfNecessary` creates missing tables only. It does not change a database that already has the `Job` table.

New installs use `utf8mb4` and store `Set.Score` as `double` (64-bit). Payload columns (`Job` arguments and invocation data, state names, reasons, state data, `Hash.Value`, `JobParameter.Value`, `Server.Data`, and `JobQueue`) use `utf8mb4_unicode_520_ci`. Identifier columns use `utf8mb4_bin`:

- `Counter.Key`, `AggregatedCounter.Key`
- `DistributedLock.Resource`
- `Hash.Key`, `Hash.Field`
- `JobParameter.Name`
- `Server.Id`
- `Set.Key`, `Set.Value`
- `List.Key`, `List.Value`

MySQL `utf8` is `utf8mb3` and cannot store emoji: a strict `sql_mode` rejects the insert, while a non-strict one silently stores `?` instead (migrating cannot recover data already replaced). `utf8mb4_general_ci` stores emoji but treats every non-BMP character as equal, so `AddToSet` (`ON DUPLICATE KEY UPDATE`) overwrites values that differ only by emoji. `utf8mb4_unicode_520_ci` keeps different emoji distinct and stays case- and accent-insensitive. It still ignores some invisible characters, so values that differ only by a zero-width joiner (U+200D) or a variation selector such as U+FE0F (for example `❤` and `❤️`) compare equal, as do `ß` and `ss`. No case-insensitive MySQL collation separates those pairs. `utf8mb4_0900_as_cs` is case-sensitive and still ignores U+FE0F and U+200D, and it requires MySQL 8. Identifier columns are `utf8mb4_bin` so `AddToSet` and `SetRangeInHash` keep each pair, with its own score or hash value.

Comparisons on those columns are case-sensitive. `Daily` and `daily` are different set values, so recurring job ids, hash fields, counter keys, server ids and lock names match exactly. `JobQueue.Queue` stays `utf8mb4_unicode_520_ci`. Hangfire 1.6 accepts only `[a-z0-9_]+` in `EnqueuedState`, but `BackgroundJobServerOptions.Queues` is not normalized, and dequeue matches with `Queue in @queues`. A server listening on `Default` still receives jobs stored as `default`. Those names cannot contain the characters `unicode_520_ci` folds.

MySQL `FLOAT` is 32-bit, so delayed and recurring scores lose precision. String columns are `VARCHAR` or `LONGTEXT`, so they can use `utf8mb4`. MySQL `NVARCHAR` is fixed to `utf8mb3`. C# already reads and writes scores as `double`.

Back up the database before migrating. These `ALTER TABLE` statements rebuild each table and block writes until the rebuild finishes, so run them during downtime. The statements are not transactional: if one fails, the tables already altered stay altered. Fix the problem and run the whole block again; running it twice is safe.

Identifier columns are altered straight to `utf8mb4_bin`. That collation keeps case, `ß` and `ss`, and invisible characters distinct, so rows that were distinct under `utf8_general_ci` stay distinct when the unique key is rebuilt. If an earlier migration already converted those columns to `utf8mb4_unicode_520_ci`, pairs it treated as equal are already stored as one row. `utf8mb4_bin` leaves that row as it is.

To upgrade an older schema, run this once. The same block is the follow-up when the tables are already `utf8mb4_unicode_520_ci`. If `TablesPrefix` is set, prefix each table name (for example `HangfireJob`). Widening `Score` keeps stored values but cannot restore precision already lost. The `Set` unique key is 1424 bytes, so the tables need the `DYNAMIC` row format, which is the default on MySQL 5.7 and later. The previous schema already needed it. On MySQL 5.6 that means `innodb_large_prefix=ON`, `innodb_file_format=Barracuda` and adding `ROW_FORMAT=DYNAMIC` to the tables yourself, because `Install.sql` does not set a row format.

```sql
SET FOREIGN_KEY_CHECKS = 0;
ALTER TABLE `Job` CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_520_ci;
ALTER TABLE `Counter`
  MODIFY `Key` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
  CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_520_ci;
ALTER TABLE `AggregatedCounter`
  MODIFY `Key` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
  CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_520_ci;
ALTER TABLE `DistributedLock`
  MODIFY `Resource` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
  CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_520_ci;
ALTER TABLE `Hash`
  MODIFY `Key` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
  MODIFY `Field` varchar(40) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
  MODIFY `Value` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_520_ci NULL,
  CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_520_ci;
ALTER TABLE `JobParameter`
  MODIFY `Name` varchar(40) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
  MODIFY `Value` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_520_ci NULL,
  CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_520_ci;
ALTER TABLE `JobQueue` CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_520_ci;
ALTER TABLE `JobState` CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_520_ci;
ALTER TABLE `Server`
  MODIFY `Id` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
  MODIFY `Data` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_520_ci NOT NULL,
  CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_520_ci;
ALTER TABLE `Set`
  MODIFY `Key` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
  MODIFY `Value` varchar(256) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
  MODIFY `Score` double NOT NULL,
  CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_520_ci;
ALTER TABLE `State` CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_520_ci;
ALTER TABLE `List`
  MODIFY `Key` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
  MODIFY `Value` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NULL,
  CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_520_ci;
SET FOREIGN_KEY_CHECKS = 1;
```


