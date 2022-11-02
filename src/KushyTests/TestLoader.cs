using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Kusto.Language;
using Kushy;
using Kusto.Language.Symbols;
using System.Threading;

namespace KushyTests
{
    public class TestLoader : SymbolLoader
    {
        public override string DefaultCluster { get; }
        public override string DefaultDomain { get; }

        private readonly IReadOnlyList<ClusterSymbol> _clusters;

        public TestLoader(IReadOnlyList<ClusterSymbol> clusters, string defaultCluster, string defaultDomain = null)
        {
            this.DefaultDomain = defaultDomain ?? KustoFacts.KustoWindowsNet;
            this.DefaultCluster = SymbolFacts.GetFullHostName(defaultCluster, this.DefaultDomain);
            _clusters = clusters;
        }

        public override Task<string[]> GetDatabaseNamesAsync(string clusterName = null, bool throwOnError = false, CancellationToken cancellationToken = default)
        {
            var cluster = _clusters.FirstOrDefault(c => c.Name == clusterName);
            if (cluster != null)
            {
                return Task.FromResult(cluster.Databases.Select(d => d.Name).ToArray());
            }

            else
            {
                return Task.FromResult(new string[0]);
            }
        }

        public override Task<DatabaseSymbol> LoadDatabaseAsync(string databaseName, string clusterName = null, bool throwOnError = false, CancellationToken cancellationToken = default)
        {
            clusterName ??= this.DefaultCluster;

            var cluster = _clusters.FirstOrDefault(c => c.Name == clusterName);
            if (cluster != null)
            {
                var db = cluster.GetDatabase(databaseName);
                return Task.FromResult(db);
            }

            return Task.FromResult<DatabaseSymbol>(null);
        }
    }
}