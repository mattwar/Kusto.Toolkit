using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Kusto.Language;
using Kusto.Language.Symbols;
using Kusto.Toolkit;

namespace Tests
{
    public class TestSymbolLoader : SymbolLoader
    {
        public override string DefaultCluster { get; }
        public override string DefaultDomain { get; }

        private readonly IReadOnlyList<ClusterSymbol> _clusters;

        public TestSymbolLoader(IReadOnlyList<ClusterSymbol> clusters, string defaultCluster = null, string defaultDomain = null)
        {
            this.DefaultDomain = defaultDomain ?? KustoFacts.KustoWindowsNet;
            this.DefaultCluster = defaultCluster != null
                ? SymbolFacts.GetFullHostName(defaultCluster, this.DefaultDomain)
                : clusters[0].Name;
            _clusters = clusters;
        }

        public override Task<IReadOnlyList<DatabaseName>> LoadDatabaseNamesAsync(string clusterName = null, CancellationToken cancellationToken = default)
        {
            var cluster = _clusters.FirstOrDefault(c => c.Name == clusterName);
            if (cluster != null)
            {
                return Task.FromResult<IReadOnlyList<DatabaseName>>(cluster.Databases.Select(d => new DatabaseName(d.Name, d.AlternateName)).ToArray());
            }

            else
            {
                return Task.FromResult<IReadOnlyList<DatabaseName>>(new DatabaseName[0]);
            }
        }

        public override Task<DatabaseSymbol> LoadDatabaseAsync(string databaseName, string clusterName = null, CancellationToken cancellationToken = default)
        {
            clusterName ??= this.DefaultCluster;

            var cluster = _clusters.FirstOrDefault(c => c.Name == clusterName);
            if (cluster != null)
            {
                var db = cluster.GetDatabase(databaseName);
                if (db != null)
                {
                    // make a new instance to behave like server/file loaders
                    db = db.WithMembers(db.Members.ToList());
                }
                return Task.FromResult(db);
            }

            return Task.FromResult<DatabaseSymbol>(null);
        }
    }
}