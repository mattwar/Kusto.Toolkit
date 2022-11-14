using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using Kusto.Language;
using Kusto.Language.Symbols;

#nullable disable // some day...

namespace Kusto.Toolkit
{
    using static SymbolFacts;

    /// <summary>
    /// A class that retrieves schema information from a cluster as <see cref="Symbol"/> instances.
    /// </summary>
    public abstract class SymbolLoader : IDisposable
    {
        public abstract string DefaultCluster { get; }
        public abstract string DefaultDomain { get; }

        /// <summary>
        /// Loads a list of database names for the specified cluster.
        /// If the cluster name is not specified, the loader's default cluster name is used.
        /// Returns null if the cluster is not found.
        /// </summary>
        public abstract Task<IReadOnlyList<DatabaseName>> LoadDatabaseNamesAsync(string clusterName = null, bool throwOnError = false, CancellationToken cancellationToken = default);

        /// <summary>
        /// Loads the corresponding database's schema and returns a new <see cref="DatabaseSymbol"/> initialized from it.
        /// If the cluster name is not specified, the loader's default cluster name is used.
        /// Returns null if the database is not found.
        /// </summary>
        public abstract Task<DatabaseSymbol> LoadDatabaseAsync(string databaseName, string clusterName = null, bool throwOnError = false, CancellationToken cancellationToken = default);

        /// <summary>
        /// Adds or updates the specified database symbol with a newly loaded version and returns a new <see cref="GlobalState"/> containing it if successful.
        /// If the cluster name is not specified, the loader's default cluster name is used.
        /// </summary>
        public Task<GlobalState> AddOrUpdateDatabaseAsync(GlobalState globals, string databaseName, string clusterName = null, bool throwOnError = false, CancellationToken cancellation = default)
        {
            return AddOrUpdateDatabaseAsync(globals, databaseName, clusterName, asDefault: false, throwOnError, cancellation);
        }

        /// <summary>
        /// Adds or updates the specified database symbol with a newly loaded version and returns a new <see cref="GlobalState"/> containing it if successful.
        /// If the cluster name is not specified, the loader's default cluster name is used.
        /// Also makes the database the default database.
        /// </summary>
        public Task<GlobalState> AddOrUpdateDefaultDatabaseAsync(GlobalState globals, string databaseName, string clusterName = null, bool throwOnError = false, CancellationToken cancellation = default)
        {
            return AddOrUpdateDatabaseAsync(globals, databaseName, clusterName, asDefault: true, throwOnError, cancellation);
        }

        private async Task<GlobalState> AddOrUpdateDatabaseAsync(GlobalState globals, string databaseName, string clusterName, bool asDefault, bool throwOnError, CancellationToken cancellation)
        {
            if (string.IsNullOrEmpty(clusterName))
            {
                clusterName = this.DefaultCluster;
            }

            clusterName = GetFullHostName(clusterName, this.DefaultDomain);

            var db = await LoadDatabaseAsync(databaseName, clusterName, throwOnError, cancellation).ConfigureAwait(false);
            if (db == null)
                return globals;

            var cluster = globals.GetCluster(clusterName);
            if (cluster == null)
            {
                cluster = new ClusterSymbol(clusterName, new[] { db });
                globals = globals.AddOrReplaceCluster(cluster);
            }
            else
            {
                cluster = cluster.AddOrUpdateDatabase(db);
                globals = globals.AddOrReplaceCluster(cluster);
            }

            if (asDefault)
            {
                globals = globals.WithCluster(cluster).WithDatabase(db);
            }

            return globals;
        }

        /// <summary>
        /// Adds or updates a cluster symbol of the specified name with open/empty database symbols for databases it does not already contain.
        /// If the cluster name is not specified, the loader's default cluster name is used.
        /// </summary>
        public Task<GlobalState> AddOrUpdateClusterAsync(GlobalState globals, string clusterName = null, bool throwOnError = false, CancellationToken cancellationToken = default)
        {
            return AddOrUpdateClusterAsync(globals, clusterName, asDefault: false, throwOnError, cancellationToken);
        }

        /// <summary>
        /// Adds or updates a cluster symbol of the specified name with open/empty database symbols for databases it does not already contain.
        /// If the cluster name is not specified, the loader's default cluster name is used.
        /// Also makes the cluster the default cluster in the returned globals.
        /// </summary>
        public Task<GlobalState> AddOrUpdateDefaultClusterAsync(GlobalState globals, string clusterName = null, bool throwOnError = false, CancellationToken cancellationToken = default)
        {
            return AddOrUpdateClusterAsync(globals, clusterName, asDefault: true, throwOnError, cancellationToken);
        }

        private async Task<GlobalState> AddOrUpdateClusterAsync(GlobalState globals, string clusterName, bool asDefault, bool throwOnError, CancellationToken cancellationToken)
        {
            if (clusterName == null)
            {
                clusterName = this.DefaultCluster;
            }

            clusterName = GetFullHostName(clusterName, this.DefaultDomain);

            var databaseNames = await this.LoadDatabaseNamesAsync(clusterName, throwOnError).ConfigureAwait(false);
            if (databaseNames != null)
            {
                var cluster = globals.GetCluster(clusterName);
                if (cluster != null)
                {
                    // only update cluster if it is missing one of the databases
                    if (databaseNames.Any(db => cluster.GetDatabase(db.Name) == null))
                    {
                        var newDbList = databaseNames.Select(db => cluster.GetDatabase(db.Name) ?? new DatabaseSymbol(db.Name, db.PrettyName, null, isOpen: true)).ToList();
                        cluster = cluster.WithDatabases(newDbList);
                        globals = globals.AddOrReplaceCluster(cluster);
                    }
                }
                else
                {
                    // initially populate with empty/open databases. These will get updated to full schema by resolver if referenced
                    var databases = databaseNames.Select(db => new DatabaseSymbol(db.Name, db.PrettyName, null, isOpen: true)).ToArray();
                    cluster = new ClusterSymbol(clusterName, databases);
                    globals = globals.AddOrReplaceCluster(cluster);
                }

                if (cluster != null && asDefault)
                {
                    globals = globals.WithCluster(cluster);
                }
            }

            return globals;
        }

        public virtual void Dispose() { }
    }

    public class DatabaseName
    {
        public string Name { get; }
        public string PrettyName { get; }

        public DatabaseName(string name, string prettyName)
        {
            this.Name = name;
            this.PrettyName = prettyName ?? "";
        }
    }
}
