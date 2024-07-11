using NLog;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Humanizer;
using NPoco;
using SyncChanges.Model;
using TableInfo = SyncChanges.Model.TableInfo;

namespace SyncChanges;

/// <summary>
/// 
/// </summary>
public class LocalToLocalSynchronizer : Synchronizer {

    static readonly Logger Log = LogManager.GetCurrentClassLogger();

    /// <summary>
    /// Ejecuta la sincronización entre una BD local hacia una o varias locales
    /// </summary>
    /// <param name="config"></param>
    public LocalToLocalSynchronizer(Config config) : base(config) { }

    /// <summary>
    /// Replica los cambios hacia las BDs destino
    /// </summary>
    /// <param name="source"></param>
    /// <param name="destinations"></param>
    /// <param name="tables"></param>
    protected override Task Replicate(DatabaseInfo source, IGrouping<long, DatabaseInfo> destinations,
        IList<TableInfo> tables) {
        var changeInfo = RetrieveChanges(source, destinations, tables);
        if (changeInfo == null || changeInfo.Changes.Count == 0) return Task.CompletedTask;

        // replicate changes to destinations
        foreach (var destination in destinations)
        {
            try
            {
                Log.Info($"Replicating {"change".ToQuantity(changeInfo.Changes.Count)} to destination {destination.Name}");

                using var db = GetDatabase(destination.ConnectionString, DatabaseType.SqlServer2012);
                using var transaction = db.GetTransaction(System.Data.IsolationLevel.ReadUncommitted);

                try
                {
                    var changes = changeInfo.Changes;
                    var disabledForeignKeyConstraints = new Dictionary<ForeignKeyConstraint, long>();

                    for (int i = 0; i < changes.Count; i++)
                    {
                        var change = changes[i];
                        Log.Debug($"Replicating change #{i + 1} of {changes.Count} (Version {change.Version}, CreationVersion {change.CreationVersion})");

                        foreach (var fk in change.ForeignKeyConstraintsToDisable)
                        {
                            if (disabledForeignKeyConstraints.TryGetValue(fk.Key, out long untilVersion))
                            {
                                // FK is already disabled, check if it needs to be deferred further than currently planned
                                if (fk.Value > untilVersion)
                                    disabledForeignKeyConstraints[fk.Key] = fk.Value;
                            } else
                            {
                                DisableForeignKeyConstraint(db, fk.Key);
                                disabledForeignKeyConstraints[fk.Key] = fk.Value;
                            }
                        }

                        PerformChange(db, change);

                        if ((i + 1) >= changes.Count || changes[i + 1].CreationVersion > change.CreationVersion) // there may be more than one change with the same CreationVersion
                        {
                            foreach (var fk in disabledForeignKeyConstraints.Where(f => f.Value <= change.CreationVersion).Select(f => f.Key).ToList())
                            {
                                ReenableForeignKeyConstraint(db, fk);
                                disabledForeignKeyConstraints.Remove(fk);
                            }
                        }
                    }

                    if (!DryRun)
                    {
                        SetSyncVersion(db, changeInfo.Version);
                        transaction.Complete();
                    }

                    Log.Info($"Destination {destination.Name} now at version {changeInfo.Version}");
                }
                catch (Exception ex)
                {
                    Error = true;
                    Log.Error(ex, $"Error replicating changes to destination {destination.Name}");
                }
            }
            catch (Exception ex)
            {
                Error = true;
                Log.Error(ex, $"Error replicating changes to destination {destination.Name}");
            }
        }

        return Task.CompletedTask;
    }

    /// <summary>
    /// Obtiene el detalle de los cambios ocurridos en la BD Primary
    /// </summary>
    /// <param name="source"></param>
    /// <param name="destinations"></param>
    /// <param name="tables"></param>
    /// <returns></returns>
    protected override ChangeInfo RetrieveChanges(DatabaseInfo source, IGrouping<long, DatabaseInfo> destinations, IList<TableInfo> tables)
    {
        var destinationVersion = destinations.Key;
        var changeInfo = new ChangeInfo();
        var changes = new List<Change>();

        using (var db = GetDatabase(source.ConnectionString, DatabaseType.SqlServer2012))
        {
            var snapshotIsolationEnabled = db.ExecuteScalar<int>("select snapshot_isolation_state from sys.databases where name = DB_NAME()") == 1;
            if (snapshotIsolationEnabled)
            {
                Log.Info($"Snapshot isolation is enabled in database {source.Name}");
                db.BeginTransaction(System.Data.IsolationLevel.Snapshot);
            } else
                Log.Info($"Snapshot isolation is not enabled in database {source.Name}, ignoring all changes above current version");

            changeInfo.Version = db.ExecuteScalar<long>("select CHANGE_TRACKING_CURRENT_VERSION()");
            Log.Info($"Current version of database {source.Name} is {changeInfo.Version}");

            foreach (var table in tables)
            {
                var tableName = table.Name;
                var minVersion = db.ExecuteScalar<long?>("select CHANGE_TRACKING_MIN_VALID_VERSION(OBJECT_ID(@0))", tableName);

                Log.Info($"Minimum version of table {tableName} in database {source.Name} is {minVersion}");

                if (minVersion > destinationVersion)
                {
                    Log.Error($"Cannot replicate table {tableName} to {"destination".ToQuantity(destinations.Count(), ShowQuantityAs.None)} {string.Join(", ", destinations.Select(d => d.Name))} because minimum source version {minVersion} is greater than destination version {destinationVersion}");
                    Error = true;
                    return null;
                }

                var sql = $@"select c.SYS_CHANGE_OPERATION, c.SYS_CHANGE_VERSION, c.SYS_CHANGE_CREATION_VERSION,
                        {string.Join(", ", table.KeyColumns.Select(c => "c." + c).Concat(table.OtherColumns.Select(c => "t." + c)))}
                        from CHANGETABLE (CHANGES {tableName}, @0) c
                        left outer join {tableName} t on ";
                sql += string.Join(" and ", table.KeyColumns.Select(k => $"c.{k} = t.{k}"));
                sql += " order by coalesce(c.SYS_CHANGE_CREATION_VERSION, c.SYS_CHANGE_VERSION), c.SYS_CHANGE_OPERATION";

                Log.Debug($"Retrieving changes for table {tableName}: {sql}");

                db.OpenSharedConnection();
                var cmd = db.CreateCommand(db.Connection, System.Data.CommandType.Text, sql, destinationVersion);

                using var reader = cmd.ExecuteReader();
                var numChanges = 0;

                while (reader.Read())
                {
                    var col = 0;
                    var change = new Change { Operation = ((string)reader[col])[0], Table = table };
                    col++;
                    var version = reader.GetInt64(col);
                    change.Version = version;
                    col++;
                    var creationVersion = reader.IsDBNull(col) ? version : reader.GetInt64(col);
                    change.CreationVersion = creationVersion;
                    col++;

                    if (!snapshotIsolationEnabled && Math.Min(version, creationVersion) > changeInfo.Version)
                    {
                        Log.Warn($"Ignoring change version {Math.Min(version, creationVersion)}");
                        continue;
                    }

                    for (int i = 0; i < table.KeyColumns.Count; i++, col++)
                        change.Keys[table.KeyColumns[i]] = reader.GetValue(col);
                    for (int i = 0; i < table.OtherColumns.Count; i++, col++)
                        change.Others[table.OtherColumns[i]] = reader.GetValue(col);

                    changes.Add(change);
                    numChanges++;
                }

                if (numChanges > 0) {
                    Log.Warn($"Table {tableName} has {"change".ToQuantity(numChanges)}");
                } else {
                    Log.Info($"Table {tableName} has {"change".ToQuantity(numChanges)}");
                }

            }

            if (snapshotIsolationEnabled)
                db.CompleteTransaction();
        }

        changeInfo.Changes.AddRange(changes.OrderBy(c => c.Version).ThenBy(c => c.Table.Name));

        ComputeForeignKeyConstraintsToDisable(changeInfo);

        return changeInfo;
    }

    protected override long GetCurrentVersion(DatabaseInfo dbInfo) {
        if (dbInfo == null) return 0;
        if (dbInfo.IsSource) 
            return GetCurrentVersionFromSource(dbInfo);
        else
            return GetCurrentVersionFromDestination(dbInfo);
    }

}