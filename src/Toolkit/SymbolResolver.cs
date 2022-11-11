using Kusto.Language;
using Kusto.Language.Editor;
using Kusto.Language.Symbols;

#nullable disable // some day...

namespace Kusto.Toolkit
{
    /// <summary>
    /// A class that resolves cluster/database references in kusto queries using a <see cref="SymbolLoader"/>
    /// </summary>
    public class SymbolResolver
    {
        private readonly SymbolLoader _loader;
        private readonly HashSet<string> _ignoreClusterNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        /// <summary>
        /// Creates a new <see cref="SymbolResolver"/> instance that is used to resolve cluster/database references in kusto queries.
        /// </summary>
        public SymbolResolver(SymbolLoader loader)
        {
            _loader = loader;
        }

        /// <summary>
        /// Loads and adds the <see cref="DatabaseSymbol"/> for any database explicity referenced in the query but not already present in the <see cref="GlobalState"/>.
        /// </summary>
        public async Task<KustoCode> AddReferencedDatabasesAsync(KustoCode code, bool throwOnError = false, CancellationToken cancellationToken = default)
        {
            // this only works if analysis is performed
            if (!code.HasSemantics)
            {
                code = code.Analyze();
            }

            // keep looping until no more databases are added to globals
            while (true)
            {
                var prevGlobals = code.Globals;

                var service = new KustoCodeService(code);
                var globals = await AddReferencedDatabasesAsync(code.Globals, service, throwOnError, cancellationToken).ConfigureAwait(false);

                if (globals == prevGlobals)
                    return code;

                code = code.WithGlobals(globals);
                code = code.Analyze(cancellationToken: cancellationToken);
            }
        }

        /// <summary>
        /// Loads and adds the <see cref="DatabaseSymbol"/> for any database explicity referenced in the <see cref="CodeScript"/> document but not already present in the <see cref="GlobalState"/>.
        /// </summary>
        public async Task<CodeScript> AddReferencedDatabasesAsync(CodeScript script, bool throwOnError = false, CancellationToken cancellationToken = default)
        {
            // keep looping until no more databases are added to globals
            while (true)
            {
                var prevGlobals = script.Globals;

                var currentGlobals = script.Globals;
                foreach (var block in script.Blocks)
                {
                    currentGlobals = await AddReferencedDatabasesAsync(currentGlobals, block.Service, throwOnError, cancellationToken).ConfigureAwait(false);
                }

                // if nothing was added we are done
                if (currentGlobals == prevGlobals)
                    return script;

                script = script.WithGlobals(currentGlobals);
            }
        }

        /// <summary>
        /// Loads and adds the <see cref="DatabaseSymbol"/> for any database explicity referenced in the query but not already present in the <see cref="GlobalState"/>.
        /// </summary>
        private async Task<GlobalState> AddReferencedDatabasesAsync(GlobalState globals, CodeService service, bool throwOnError = false, CancellationToken cancellationToken = default)
        {
            // find all explicit cluster (xxx) references
            var clusterRefs = service.GetClusterReferences(cancellationToken);
            foreach (ClusterReference clusterRef in clusterRefs)
            {
                var clusterName = SymbolFacts.GetFullHostName(clusterRef.Cluster, _loader.DefaultDomain);

                // don't bother with cluster names that we've already shown to not exist
                if (_ignoreClusterNames.Contains(clusterName))
                    continue;

                var cluster = globals.GetCluster(clusterName);
                if (cluster == null || cluster.IsOpen)
                {
                    // check to see if this is an actual cluster and get all database names
                    var databaseNames = await _loader.GetDatabaseNamesAsync(clusterName, throwOnError).ConfigureAwait(false);
                    if (databaseNames != null)
                    {
                        // initially populate with empty 'open' databases. These will get updated to full schema if referenced
                        var databases = databaseNames.Select(db => new DatabaseSymbol(db.Name, db.PrettyName, null, isOpen: true)).ToArray();
                        cluster = new ClusterSymbol(clusterName, databases);
                        globals = globals.WithClusterList(globals.Clusters.Concat(new[] { cluster }).ToArray());
                    }
                }

                // we already have all the known schema for this cluster
                _ignoreClusterNames.Add(clusterName);
            }

            // examine all explicit database(xxx) references
            var dbRefs = service.GetDatabaseReferences(cancellationToken);
            foreach (DatabaseReference dbRef in dbRefs)
            {
                var clusterName = string.IsNullOrEmpty(dbRef.Cluster)
                    ? null
                    : SymbolFacts.GetFullHostName(dbRef.Cluster, _loader.DefaultDomain);

                // get implicit or explicit named cluster
                var cluster = string.IsNullOrEmpty(clusterName)
                    ? globals.Cluster
                    : globals.GetCluster(clusterName);

                if (cluster != null)
                {
                    var db = cluster.GetDatabase(dbRef.Database);

                    // is this one of those not-yet-populated databases?
                    if (db == null || (db != null && db.Members.Count == 0 && db.IsOpen))
                    {
                        var newGlobals = await _loader.AddOrUpdateDatabaseAsync(globals, dbRef.Database, clusterName, throwOnError, cancellationToken).ConfigureAwait(false);
                        globals = newGlobals != null ? newGlobals : globals;
                    }
                }
            }

            return globals;
        }
    }
}
