using Kusto.Data;
using Kusto.Language.Symbols;

#nullable disable // some day...

namespace Kusto.Toolkit
{
    /// <summary>
    /// A <see cref="SymbolLoader"/> that maintains a file based cache of database schemas.
    /// </summary>
    public class CachedSymbolLoader : SymbolLoader
    {
        public FileSymbolLoader FileLoader { get; }
        public ServerSymbolLoader ServerLoader { get; }
        private bool _autoDispose;

        public CachedSymbolLoader(ServerSymbolLoader serverLoader, FileSymbolLoader fileLoader, bool autoDispose = true)
        {
            if (serverLoader == null)
                throw new ArgumentNullException(nameof(serverLoader));

            if (fileLoader == null)
                throw new ArgumentNullException(nameof(fileLoader));

            this.ServerLoader = serverLoader;
            this.FileLoader = fileLoader;
            _autoDispose = autoDispose;
        }

        public CachedSymbolLoader(KustoConnectionStringBuilder connectionBuilder, string cachePath, string defaultDomain = null)
        {
            if (connectionBuilder == null)
                throw new ArgumentNullException(nameof(connectionBuilder));

            if (cachePath == null)
                throw new ArgumentNullException(nameof(cachePath));

            this.ServerLoader = new ServerSymbolLoader(connectionBuilder, defaultDomain);
            this.FileLoader = new FileSymbolLoader(cachePath, this.ServerLoader.DefaultCluster, defaultDomain);
            _autoDispose = true;
        }

        public CachedSymbolLoader(string connection, string cachePath, string defaultDomain = null)
        {
            if (connection == null)
                throw new ArgumentNullException(nameof(connection));

            if (cachePath == null)
                throw new ArgumentNullException(nameof(cachePath));

            this.ServerLoader = new ServerSymbolLoader(connection, defaultDomain);
            this.FileLoader = new FileSymbolLoader(cachePath, this.ServerLoader.DefaultCluster, defaultDomain);
            _autoDispose = true;
        }

        public override string DefaultCluster => this.ServerLoader.DefaultCluster;
        public override string DefaultDomain => this.ServerLoader.DefaultDomain;

        public override void Dispose()
        {
            if (_autoDispose)
            {
                this.FileLoader.Dispose();
                this.ServerLoader.Dispose();
            }
        }

        /// <summary>
        /// Loads a list of database names for the specified cluster.
        /// If the cluster name is not specified, the loader's default cluster name is used.
        /// Returns null if the cluster is not found.
        /// </summary>
        public override async Task<IReadOnlyList<DatabaseName>> LoadDatabaseNamesAsync(string clusterName = null, bool throwOnError = false, CancellationToken cancellationToken = default)
        {
            var dbNames = await this.FileLoader.LoadDatabaseNamesAsync(clusterName, cancellationToken: cancellationToken).ConfigureAwait(false);

            if (dbNames == null)
            {
                dbNames = await this.ServerLoader.LoadDatabaseNamesAsync(clusterName, throwOnError, cancellationToken).ConfigureAwait(false);

                if (dbNames != null)
                {
                    await this.FileLoader.SaveDatabaseNamesAsync(dbNames, clusterName, throwOnError, cancellationToken).ConfigureAwait(false);
                }
            }

            return dbNames;
        }

        /// <summary>
        /// Loads the corresponding database's schema and returns a new <see cref="DatabaseSymbol"/> initialized from it.
        /// If the cluster name is not specified, the loader's default cluster name is used.
        /// Returns null if the database is not found.
        /// </summary>
        public override async Task<DatabaseSymbol> LoadDatabaseAsync(string databaseName, string clusterName = null, bool throwOnError = false, CancellationToken cancellationToken = default)
        {
            var db = await this.FileLoader.LoadDatabaseAsync(databaseName, clusterName, false, cancellationToken).ConfigureAwait(false);

            if (db == null)
            {
                db = await this.ServerLoader.LoadDatabaseAsync(databaseName, clusterName, throwOnError, cancellationToken).ConfigureAwait(false);

                if (db != null)
                {
                    await this.FileLoader.SaveDatabaseAsync(db, clusterName, throwOnError, cancellationToken).ConfigureAwait(false);
                }
            }

            return db;
        }
    }
}
