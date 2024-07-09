using NLog;
using System;
using System.Collections.Generic;
using System.Linq;
using Humanizer;
using NPoco;

namespace SyncChanges;

/// <summary>
/// 
/// </summary>
public class LocalToRemoteSynchronizer : Synchronizer {

    static readonly Logger Log = LogManager.GetCurrentClassLogger();

    /// <summary>
    /// Ejecuta la sincronización entre una BD local hacia una o varias locales
    /// </summary>
    /// <param name="config"></param>
    public LocalToRemoteSynchronizer(Config config) : base(config) { }

    /// <summary>
    /// Replica los cambios hacia destinos remotos, haciendo uso del Replicator y el Broker
    /// </summary>
    /// <param name="source"></param>
    /// <param name="destinations"></param>
    /// <param name="tables"></param>
    protected override void Replicate(DatabaseInfo source, IGrouping<long, DatabaseInfo> destinations, IList<TableInfo> tables) {
        ChangeInfo changeInfo = RetrieveChanges(source, destinations, tables);
        if (changeInfo == null || changeInfo.Changes.Count == 0) return;

        try
        {
            Log.Info($"Replicating {"change".ToQuantity(changeInfo.Changes.Count)} to Remote Destination");

            // replicate changes to the remote destination via remote broker

            var fileName = PersistChangeInfo(changeInfo);
            Log.Info($"ChangeInfo saved: {fileName}");

            var changeInfoToVal = LoadChangeInfo(fileName);
            if (changeInfoToVal == null || changeInfoToVal.Changes.Count != changeInfo.Changes.Count) {
                throw new ApplicationException($"{fileName} file is invalid");
            }
            Log.Info($"ChangeInfo is valid: {"change".ToQuantity(changeInfo.Changes.Count)}");

            TransferToBroker(fileName);

        }
#pragma warning disable CA1031 // Do not catch general exception types
        catch (Exception ex)
        {
            Error = true;
            Log.Error(ex, $"Error replicating changes to Remote Destination");
        }
#pragma warning restore CA1031 // Do not catch general exception types

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
                table.MinVersion = minVersion ?? 0;
                Log.Info($"Minimum version of table {tableName} in database {source.Name} is {minVersion}");

                //if (minVersion > destinationVersion)
                //{
                //    Log.Error($"Cannot replicate table {tableName} to {"destination".ToQuantity(destinations.Count(), ShowQuantityAs.None)} {string.Join(", ", destinations.Select(d => d.Name))} because minimum source version {minVersion} is greater than destination version {destinationVersion}");
                //    Error = true;
                //    return null;
                //}

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

                if (numChanges > 0)
                {
                    Log.Warn($"Table {tableName} has {"change".ToQuantity(numChanges)}");
                } else
                {
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
        // MAG: Desde este tipo de Synchronizer debemos obtener la version desde la BD destino
        return GetCurrentVersionFromDestination(dbInfo);
    }

}