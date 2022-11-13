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
        private readonly Dictionary<string, HashSet<string>> _clustersResolvedOrInvalid
            = new Dictionary<string, HashSet<string>>(StringComparer.OrdinalIgnoreCase);

        /// <summary>
        /// Creates a new <see cref="SymbolResolver"/> instance that is used to resolve cluster/database references in kusto queries.
        /// </summary>
        public SymbolResolver(SymbolLoader loader)
        {
            _loader = loader;
        }

        /// <summary>
        /// Maximum loops allowed when checking for additional references after just adding referenced database schema.
        /// If this is exceeded then there is probably a bug that keeps updating the globals even when no new found databases are added.
        /// </summary>
        private const int MaxLoopCount = 20;

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
            for (int loopCount = 0; loopCount < MaxLoopCount; loopCount++)
            {
                var service = new KustoCodeService(code);
                var globals = await AddReferencedDatabasesAsync(code.Globals, service, throwOnError, cancellationToken).ConfigureAwait(false);

                // if no databases were added we are done
                if (globals == code.Globals)
                    break;

                code = code.WithGlobals(globals);
                code = code.Analyze(cancellationToken: cancellationToken);
            }

            return code;
        }

        /// <summary>
        /// Loads and adds the <see cref="DatabaseSymbol"/> for any database explicity referenced in the <see cref="CodeScript"/> document but not already present in the <see cref="GlobalState"/>.
        /// </summary>
        public async Task<CodeScript> AddReferencedDatabasesAsync(CodeScript script, bool throwOnError = false, CancellationToken cancellationToken = default)
        {
            // keep looping until no more databases are added to globals
            for (int loopCount = 0; loopCount < MaxLoopCount; loopCount++)
            {
                var currentGlobals = script.Globals;
                foreach (var block in script.Blocks)
                {
                    currentGlobals = await AddReferencedDatabasesAsync(currentGlobals, block.Service, throwOnError, cancellationToken).ConfigureAwait(false);
                }

                // if no databases were added we are done
                if (currentGlobals == script.Globals)
                    break;

                script = script.WithGlobals(currentGlobals);
            }

            return script;
        }

        /// <summary>
        /// Loads and adds the <see cref="DatabaseSymbol"/> for any database explicity referenced in the query but not already present in the <see cref="GlobalState"/>.
        /// </summary>
        private async Task<GlobalState> AddReferencedDatabasesAsync(GlobalState globals, CodeService service, bool throwOnError = false, CancellationToken cancellationToken = default)
        {
            // find all explicit cluster('xxx') references
            var clusterRefs = service.GetClusterReferences(cancellationToken);
            foreach (ClusterReference clusterRef in clusterRefs)
            {
                var clusterName = SymbolFacts.GetFullHostName(clusterRef.Cluster, _loader.DefaultDomain);

                // don't bother with clusters were already resolved or do not exist
                if (string.IsNullOrEmpty(clusterName)
                    || _clustersResolvedOrInvalid.ContainsKey(clusterName))
                    continue;

                _clustersResolvedOrInvalid.Add(clusterName, new HashSet<string>());

                var cluster = globals.GetCluster(clusterName);
                if (cluster == null || cluster.IsOpen)
                {
                    globals = await _loader.AddOrUpdateClusterAsync(globals, clusterName, throwOnError, cancellationToken).ConfigureAwait(false);
                }
            }

            // find all explicit database('xxx') references
            var dbRefs = service.GetDatabaseReferences(cancellationToken);
            foreach (DatabaseReference dbRef in dbRefs)
            {
                var cluster = string.IsNullOrEmpty(dbRef.Cluster)
                    ? globals.Cluster
                    : globals.GetCluster(SymbolFacts.GetFullHostName(dbRef.Cluster, _loader.DefaultDomain));

                // don't rely on the user to do the right thing.
                if (cluster == null 
                    || cluster == ClusterSymbol.Unknown 
                    || string.IsNullOrEmpty(cluster.Name)
                    || string.IsNullOrEmpty(dbRef.Database))
                    continue;

                if (!_clustersResolvedOrInvalid.TryGetValue(cluster.Name, out var dbsResolvedOrInvalid))
                {
                    dbsResolvedOrInvalid = new HashSet<string>();
                    _clustersResolvedOrInvalid.Add(cluster.Name, dbsResolvedOrInvalid);
                }

                // don't bother with databases already resolved no do not exist
                if (dbsResolvedOrInvalid.Contains(dbRef.Database))
                    continue;

                dbsResolvedOrInvalid.Add(dbRef.Database);

                var db = cluster.GetDatabase(dbRef.Database);
                if (db == null || (db != null && db.Members.Count == 0 && db.IsOpen))
                {
                    var newGlobals = await _loader.AddOrUpdateDatabaseAsync(globals, dbRef.Database, cluster.Name, throwOnError, cancellationToken).ConfigureAwait(false);
                    globals = newGlobals ?? globals;
                }
            }

            return globals;
        }
    }
}
