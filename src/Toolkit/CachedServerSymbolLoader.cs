using Kusto.Language.Symbols;

#nullable disable // some day...

namespace Kusto.Toolkit
{
    /// <summary>
    /// A <see cref="SymbolLoader"/> that maintains a file based cache of database schemas.
    /// </summary>
    public class CachedServerSymbolLoader : SymbolLoader
    {
        public FileSymbolLoader FileLoader { get; }
        public ServerSymbolLoader ServerLoader { get; }
        private bool _autoDispose;

        public CachedServerSymbolLoader(string connection, string cachePath, string defaultDomain = null)
        {
            this.ServerLoader = new ServerSymbolLoader(connection, defaultDomain);
            this.FileLoader = new FileSymbolLoader(cachePath, this.ServerLoader.DefaultCluster, defaultDomain);
            _autoDispose = true;
        }

        public CachedServerSymbolLoader(ServerSymbolLoader serverLoader, FileSymbolLoader fileLoader, bool autoDispose = true)
        {
            this.ServerLoader = serverLoader;
            this.FileLoader = fileLoader;
            _autoDispose = autoDispose;
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

        public override async Task<IReadOnlyList<DatabaseName>> GetDatabaseNamesAsync(string clusterName = null, bool throwOnError = false, CancellationToken cancellationToken = default)
        {
            var names = await this.FileLoader.GetDatabaseNamesAsync(clusterName, cancellationToken: cancellationToken).ConfigureAwait(false);

            if (names == null)
            {
                names = await this.ServerLoader.GetDatabaseNamesAsync(clusterName, throwOnError, cancellationToken).ConfigureAwait(false);

                if (names != null)
                {
                    await this.FileLoader.SaveDatabaseNamesAsync(clusterName, names, throwOnError, cancellationToken);
                }
            }

            return names;
        }

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
