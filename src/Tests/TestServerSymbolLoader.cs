using Kusto.Data;
using Kusto.Data.Common;
using Kusto.Data.Data;
using Kusto.Data.Net.Client;
using Kusto.Toolkit;
using System.Data;
using System.Threading;
using System.Threading.Tasks;
using System.Linq;
using Kusto.Language.Symbols;
using System.Collections.Generic;

namespace Tests
{
    public class TestServerSymbolLoader : ServerSymbolLoader
    {
        private readonly object[] _results;

        public TestServerSymbolLoader(params object[] results)
            : base(new KustoConnectionStringBuilder())
        {
            _results = results;
        }

        public override Task<DatabaseSymbol> LoadDatabaseAsync(string databaseName, string clusterName = null, CancellationToken cancellationToken = default)
        {
            var result = _results.OfType<DatabaseSymbol>().FirstOrDefault();
            if (result != null)
                return Task.FromResult(result);
            return base.LoadDatabaseAsync(databaseName, clusterName, cancellationToken);
        }

        public override Task<IReadOnlyList<DatabaseName>> LoadDatabaseNamesAsync(string clusterName = null, CancellationToken cancellationToken = default)
        {
            var result = _results.OfType<IReadOnlyList<DatabaseName>>().FirstOrDefault();
            if (result != null)
                return Task.FromResult(result);
            return base.LoadDatabaseNamesAsync(clusterName, cancellationToken);
        }

        protected override Task<DatabaseName> GetBothDatabaseNamesAsync(string cluster, string databaseNameOrPrettyName, CancellationToken cancellationToken)
        {
            var result = _results.OfType<DatabaseName>().FirstOrDefault();
            if (result != null)
                return Task.FromResult(result);
            return base.GetBothDatabaseNamesAsync(cluster, databaseNameOrPrettyName, cancellationToken);
        }

        protected override Task<T[]> ExecuteControlCommandAsync<T>(string cluster, string database, string command, CancellationToken cancellationToken)
        {
            foreach (var result in _results)
            {
                if (result is T[] typedResult)
                    return Task.FromResult(typedResult);
            }

            return Task.FromResult<T[]>(null);
        }
    }
}
