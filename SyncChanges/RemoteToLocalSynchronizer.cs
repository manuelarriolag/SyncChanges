using NLog;
using System;
using System.Collections.Generic;
using System.Linq;
using Humanizer;
using NPoco;
using System.IO;
using SyncChanges.Model;
using TableInfo = SyncChanges.Model.TableInfo;

namespace SyncChanges;

/// <summary>
/// 
/// </summary>
public class RemoteToLocalSynchronizer : Synchronizer {

    static readonly Logger Log = LogManager.GetCurrentClassLogger();

    /// <summary>
    /// Ejecuta la sincronización entre una BD local hacia una o varias locales
    /// </summary>
    /// <param name="config"></param>
    public RemoteToLocalSynchronizer(Config config) : base(config) { }

    /// <summary>
    /// Replica los cambios desde destinos remotos hacia la BD local
    /// </summary>
    /// <param name="source"></param>
    /// <param name="destinations"></param>
    /// <param name="tables"></param>
    protected override void Replicate(DatabaseInfo source, IGrouping<long, DatabaseInfo> destinations, IList<TableInfo> tables) {
        ChangeInfo changeInfo = RetrieveChanges(source, destinations, tables);
        if (changeInfo == null || changeInfo.Changes.Count == 0) return;


        // replicate changes from the remote Origin via broker
        Log.Info($"Replicating {"change".ToQuantity(changeInfo.Changes.Count)} from Remote to Local");

        var destinationVersion = destinations.Key;

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

                        Log.Info($"Minimum version of table {change.Table.Name} in source database {source.Name} is {change.Table.MinVersion}");

                        // TODO Validar si esto funcionará con nuevos integrantes en la red
                        if (destinationVersion == 0)
                        {
                            destinationVersion = change.Table.MinVersion;
                            SetSyncVersion(db, change.Table.MinVersion);
                        }

                        if (change.Table.MinVersion <= 0 || change.Table.MinVersion > destinationVersion)
                        {
                            Log.Error($"Incompatible version. Cannot replicate table {change.Table.Name} to destination {destination.Name} because minimum source version {change.Table.MinVersion} is greater than destination version {destinationVersion} (filename: {changeInfo.FileName})");
                            Error = true;
                            break;
                        }

                        if (destinationVersion > changeInfo.Version)
                        {
                            Log.Error($"Version obsoleted. Cannot replicate table {change.Table.Name} to destination {destination.Name} because destination version {destinationVersion} is greater or equal than source version {changeInfo.Version} (filename: {changeInfo.FileName})");
                            Error = true;
                            continue;
                        }

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

                    Log.Info($"Success. Destination {destination.Name} now at version {changeInfo.Version} (from Remote Origin) (filename: {changeInfo.FileName})");

                }
#pragma warning disable CA1031 // Do not catch general exception types
                catch (Exception ex)
                {
                    Error = true;
                    Log.Error(ex, $"Error replicating changes to destination {destination.Name} (from Remote Origin) (filename: {changeInfo.FileName})");
                }
#pragma warning restore CA1031 // Do not catch general exception types
            }
#pragma warning disable CA1031 // Do not catch general exception types
            catch (Exception ex)
            {
                Error = true;
                Log.Error(ex, $"Error replicating changes to destination {destination.Name} (from Remote Origin) (filename: {changeInfo.FileName})");
            }
#pragma warning restore CA1031 // Do not catch general exception types
        } // foreach...

        if (!Error) {
            RenameFileName(changeInfo.FileName, CHANGEINFO_RECEIVED, CHANGEINFO_REPLICATED);
        } else {
            RenameFileName(changeInfo.FileName, CHANGEINFO_RECEIVED, CHANGEINFO_FAILED);
        }

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
        ChangeInfo changeInfo = null;
        try
        {

#if DEBUG
            // TODO Temporal, solo para pruebas
            ReceiveAllFromBroker();
#endif
            // replicate changes from the remote Origin via broker
            changeInfo = LoadPersistedChanges(CHANGEINFO_RECEIVED).FirstOrDefault();

            if (changeInfo != null) {
                Log.Info($"Retrieving {"change".ToQuantity(changeInfo.Changes.Count)} from Remote Origin to Local");
            }

        }
#pragma warning disable CA1031 // Do not catch general exception types
        catch (Exception ex)
        {
            Error = true;
            Log.Error(ex, $"Error Retrieving changes from Remote Origin");
        }

        return changeInfo;

    }

    protected override long GetCurrentVersion(DatabaseInfo dbInfo) {
        // MAG: Desde este tipo de Synchronizer debemos obtener la version desde la BD Origen
        return GetCurrentVersionFromSource(dbInfo);
    }

}