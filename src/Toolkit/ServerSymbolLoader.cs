using Kusto.Cloud.Platform.Data;
using Kusto.Data;
using Kusto.Data.Data;
using Kusto.Data.Net.Client;
using Kusto.Language;
using Kusto.Language.Symbols;
using Kusto.Data.Common;

#nullable disable // some day...

namespace Kusto.Toolkit
{
    using static SymbolFacts;

    /// <summary>
    /// A <see cref="SymbolLoader"/> that retrieves schema symbols from a Kusto server.
    /// </summary>
    public class ServerSymbolLoader : SymbolLoader
    {
        private readonly KustoConnectionStringBuilder _defaultConnection;
        private readonly string _defaultClusterName;
        private readonly string _defaultDomain;
        private readonly Dictionary<string, ICslAdminProvider> _dataSourceToAdminProviderMap = new Dictionary<string, ICslAdminProvider>();
        private readonly HashSet<string> _ignoreClusterNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        private readonly Dictionary<string, HashSet<string>> _clusterToBadDbNameMap = new Dictionary<string, HashSet<string>>();

        /// <summary>
        /// Creates a new <see cref="SymbolLoader"/> instance. recommended method: SymbolLoader(KustoConnectionStringBuilder clusterConnection)
        /// </summary>
        /// <param name="clusterConnection">The cluster connection string.</param>
        /// <param name="defaultDomain">The domain used to convert short cluster host names into full cluster host names.
        /// This string must start with a dot.  If not specified, the default domain is ".Kusto.Windows.Net"
        /// </param>
        public ServerSymbolLoader(string clusterConnection, string defaultDomain = null)
            : this(new KustoConnectionStringBuilder(clusterConnection), defaultDomain)
        {
            if (clusterConnection == null)
                throw new ArgumentNullException(nameof(clusterConnection));
        }

        /// <summary>
        /// Creates a new <see cref="SymbolLoader"/> instance.
        /// </summary>
        /// <param name="clusterConnection">The cluster connection.</param>
        /// <param name="defaultDomain">The domain used to convert short cluster host names into full cluster host names.
        /// This string must start with a dot.  If not specified, the default domain is ".Kusto.Windows.Net"
        /// </param>
        public ServerSymbolLoader(KustoConnectionStringBuilder clusterConnection, string defaultDomain = null)
        {
            if (clusterConnection == null)
                throw new ArgumentNullException(nameof(clusterConnection));

            _defaultConnection = clusterConnection;
            _defaultClusterName = GetHost(clusterConnection);
            _defaultDomain = String.IsNullOrEmpty(defaultDomain)
                ? KustoFacts.KustoWindowsNet
                : defaultDomain;
        }

        public override string DefaultDomain => _defaultDomain;

        public override string DefaultCluster => _defaultClusterName;

        /// <summary>
        /// The default database specified in the connection
        /// </summary>
        public string DefaultDatabase => _defaultConnection.InitialCatalog;

        /// <summary>
        /// Dispose any open resources.
        /// </summary>
        public override void Dispose()
        {
            // Disposes any open admin providers.
            var providers = _dataSourceToAdminProviderMap.Values.ToList();
            _dataSourceToAdminProviderMap.Clear();

            foreach (var provider in providers)
            {
                provider.Dispose();
            }
        }

        /// <summary>
        /// Gets or Creates an admin provider instance.
        /// </summary>
        private ICslAdminProvider GetOrCreateAdminProvider(KustoConnectionStringBuilder connection)
        {
            if (!_dataSourceToAdminProviderMap.TryGetValue(connection.DataSource, out var provider))
            {
                provider = KustoClientFactory.CreateCslAdminProvider(connection);
                _dataSourceToAdminProviderMap.Add(connection.DataSource, provider);
            }

            return provider;
        }

        /// <summary>
        /// Loads a list of database names for the specified cluster.
        /// If the cluster name is not specified, the loader's default cluster name is used.
        /// Returns null if the cluster is not found.
        /// </summary>
        public override async Task<IReadOnlyList<DatabaseName>> LoadDatabaseNamesAsync(string clusterName = null, bool throwOnError = false, CancellationToken cancellationToken = default)
        {
            clusterName = string.IsNullOrEmpty(clusterName)
                ? _defaultClusterName
                : GetFullHostName(clusterName, _defaultDomain);

            var connection = GetClusterConnection(clusterName);
            var provider = GetOrCreateAdminProvider(connection);

            var databases = await ExecuteControlCommandAsync<ShowDatabasesResult>(
                provider, database: "", 
                ".show databases",
                throwOnError, cancellationToken)
                .ConfigureAwait(false);

            return databases?.Select(d => new DatabaseName(d.DatabaseName, d.PrettyName)).ToArray();
        }

        private bool IsBadDatabaseName(string clusterName, string databaseName)
        {
            return _clusterToBadDbNameMap.TryGetValue(clusterName, out var badDbNames)
                && badDbNames.Contains(databaseName);
        }

        private void AddBadDatabasename(string clusterName, string databaseName)
        {
            if (!_clusterToBadDbNameMap.TryGetValue(clusterName, out var badDbNames))
            {
                badDbNames = new HashSet<string>();
                _clusterToBadDbNameMap.Add(clusterName, badDbNames);
            }

            badDbNames.Add(databaseName);
        }

        /// <summary>
        /// Loads the corresponding database's schema and returns a new <see cref="DatabaseSymbol"/> initialized from it.
        /// If the cluster name is not specified, the loader's default cluster name is used.
        /// Returns null if the database is not found.
        /// </summary>
        public override async Task<DatabaseSymbol> LoadDatabaseAsync(string databaseName, string clusterName = null, bool throwOnError = false, CancellationToken cancellationToken = default)
        {
            if (databaseName == null)
                throw new ArgumentNullException(nameof(databaseName));

            clusterName = string.IsNullOrEmpty(clusterName)
                ? _defaultClusterName
                : GetFullHostName(clusterName, _defaultDomain);

            // if we've already determined this database name is bad, then bail out
            if (IsBadDatabaseName(clusterName, databaseName))
                return null;

            var connection = GetClusterConnection(clusterName);
            var provider = GetOrCreateAdminProvider(connection);

            // get database name & pretty name from .show databases
            var dbNameLiteral = KustoFacts.GetStringLiteral(databaseName);
            var dbInfos = await ExecuteControlCommandAsync<ShowDatabasesResult>(
                provider, database: "",
                $".show databases | where DatabaseName == {dbNameLiteral} or PrettyName == {dbNameLiteral}",
                throwOnError, cancellationToken);

            var dbInfo = dbInfos?.FirstOrDefault();
            if (dbInfo == null)
            {
                AddBadDatabasename(clusterName, databaseName);
                return null;
            }

            databaseName = dbInfo.DatabaseName;

            var tables = await LoadTablesAsync(provider, databaseName, throwOnError, cancellationToken).ConfigureAwait(false);
            if (tables == null)
            {
                AddBadDatabasename(clusterName, databaseName);
                return null;
            }

            var externalTables = await LoadExternalTablesAsync(provider, databaseName, throwOnError, cancellationToken).ConfigureAwait(false);
            var materializedViews = await LoadMaterializedViewsAsync(provider, databaseName, throwOnError, cancellationToken).ConfigureAwait(false);
            var functions = await LoadFunctionsAsync(provider, databaseName, throwOnError, cancellationToken).ConfigureAwait(false);
            var entityGroups = await LoadEntityGroupsAsync(provider, databaseName, throwOnError, cancellationToken).ConfigureAwait(false);

            var members = new List<Symbol>();
            members.AddRange(tables);
            members.AddRange(externalTables);
            members.AddRange(materializedViews);
            members.AddRange(functions);
            members.AddRange(entityGroups);

            var databaseSymbol = new DatabaseSymbol(dbInfo.DatabaseName, dbInfo.PrettyName, members);
            return databaseSymbol;
        }

        private async Task<IReadOnlyList<TableSymbol>> LoadTablesAsync(ICslAdminProvider provider, string databaseName, bool throwOnError, CancellationToken cancellationToken)
        {
            var tables = new List<TableSymbol>();

            // get table schema from .show database xxx schema
            var databaseSchemas = await ExecuteControlCommandAsync<ShowDatabaseSchemaResult>(
                provider, databaseName,
                $".show database {KustoFacts.GetBracketedName(databaseName)} schema", 
                throwOnError, cancellationToken)
                .ConfigureAwait(false);
            if (databaseSchemas == null)
                return null;

            foreach (var table in databaseSchemas.Where(s => !string.IsNullOrEmpty(s.TableName)).GroupBy(s => s.TableName))
            {
                var tableDocString = table.FirstOrDefault(t => string.IsNullOrEmpty(t.ColumnName) && !string.IsNullOrEmpty(t.DocString))?.DocString;
                var columnSchemas = table.Where(t => !string.IsNullOrEmpty(t.ColumnName)).ToArray();
                var columns = columnSchemas.Select(s => new ColumnSymbol(s.ColumnName, GetKustoType(s.ColumnType), s.DocString)).ToList();
                var tableSymbol = new TableSymbol(table.Key, columns, tableDocString);
                tables.Add(tableSymbol);
            }

            return tables;
        }

        private async Task<IReadOnlyList<TableSymbol>> LoadExternalTablesAsync(ICslAdminProvider provider, string databaseName, bool throwOnError, CancellationToken cancellationToken)
        {
            var tables = new List<TableSymbol>();

            // get external tables from .show external tables and .show external table xxx cslschema
            var externalTables = await ExecuteControlCommandAsync<ShowExternalTablesResult>(provider, databaseName, ".show external tables", throwOnError, cancellationToken);
            if (externalTables != null)
            {
                foreach (var et in externalTables)
                {
                    var etSchemas = await ExecuteControlCommandAsync<ShowExternalTableSchemaResult>(
                        provider, databaseName,
                        $".show external table {KustoFacts.GetBracketedName(et.TableName)} cslschema",
                        throwOnError, cancellationToken)
                        .ConfigureAwait(false);
                    if (etSchemas != null && etSchemas.Length > 0)
                    {
                        var mvSymbol = new ExternalTableSymbol(et.TableName, "(" + etSchemas[0].Schema + ")", et.DocString);
                        tables.Add(mvSymbol);
                    }
                }
            }

            return tables;
        }

        private async Task<IReadOnlyList<TableSymbol>> LoadMaterializedViewsAsync(ICslAdminProvider provider, string databaseName, bool throwOnError, CancellationToken cancellationToken)
        {
            var tables = new List<TableSymbol>();

            // get materialized views from .show materialized-views and .show materialized-view xxx cslschema
            var materializedViews = await ExecuteControlCommandAsync<ShowMaterializedViewsResult>(
                provider, databaseName, 
                ".show materialized-views", 
                throwOnError, cancellationToken); ;
            if (materializedViews != null)
            {
                foreach (var mv in materializedViews)
                {
                    var mvSchemas = await ExecuteControlCommandAsync<ShowMaterializedViewSchemaResult>(
                        provider, databaseName, 
                        $".show materialized-view {KustoFacts.GetBracketedName(mv.Name)} cslschema", 
                        throwOnError, cancellationToken)
                        .ConfigureAwait(false);
                    if (mvSchemas != null && mvSchemas.Length > 0)
                    {
                        var mvSymbol = new MaterializedViewSymbol(mv.Name, "(" + mvSchemas[0].Schema + ")", mv.Query, mv.DocString);
                        tables.Add(mvSymbol);
                    }
                }
            }

            return tables;
        }

        private async Task<IReadOnlyList<FunctionSymbol>> LoadFunctionsAsync(ICslAdminProvider provider, string databaseName, bool throwOnError, CancellationToken cancellationToken)
        {
            var functions = new List<FunctionSymbol>();

            // get functions for .show functions
            var functionSchemas = await ExecuteControlCommandAsync<ShowFunctionsResult>(
                provider, databaseName, 
                ".show functions", 
                throwOnError, cancellationToken)
                .ConfigureAwait(false);
            if (functionSchemas == null)
                return null;

            foreach (var fun in functionSchemas)
            {
                var functionSymbol = new FunctionSymbol(fun.Name, fun.Parameters, fun.Body, fun.DocString);
                functions.Add(functionSymbol);
            }

            return functions;
        }

        private async Task<IReadOnlyList<EntityGroupSymbol>> LoadEntityGroupsAsync(ICslAdminProvider provider, string databaseName, bool throwOnError, CancellationToken cancellationToken)
        {
            var entityGroupSymbols = new List<EntityGroupSymbol>();

            // get entity groups via .show entity_groups
            var entityGroups = await ExecuteControlCommandAsync<ShowEntityGroupsResult>(
                provider, databaseName, 
                ".show entity_groups",
                throwOnError, cancellationToken)
                .ConfigureAwait(false);
            if (entityGroups == null)
                return null;

            return entityGroups.Select(eg => new EntityGroupSymbol(eg.Name, eg.Entities)).ToList();
        }

        /// <summary>
        /// Executes a query or command against a kusto cluster and returns a sequence of result row instances.
        /// </summary>
        private async Task<T[]> ExecuteControlCommandAsync<T>(ICslAdminProvider provider, string database, string command, bool throwOnError, CancellationToken cancellationToken)
        {
            try
            {
                var resultReader = await provider.ExecuteControlCommandAsync(database, command).ConfigureAwait(false);
                var results = KustoDataReaderParser.ParseV1(resultReader, null);
                var tableReader = results[WellKnownDataSet.PrimaryResult].Single().TableData.CreateDataReader();
                var objectReader = new ObjectReader<T>(tableReader);
                return objectReader.ToArray();
            }
            catch (Exception) when (!throwOnError)
            {
                return null;
            }
        }

        private string GetHost(KustoConnectionStringBuilder connection) =>
            new Uri(connection.DataSource).Host;

        private KustoConnectionStringBuilder GetClusterConnection(string clusterUriOrName)
        {
            if (string.IsNullOrEmpty(clusterUriOrName)
                || clusterUriOrName == _defaultClusterName)
            {
                return _defaultConnection;
            }

            if (string.IsNullOrWhiteSpace(clusterUriOrName))
                return null;

            var clusterUri = clusterUriOrName;

            if (!clusterUri.Contains("://"))
            {
                clusterUri = _defaultConnection.ConnectionScheme + "://" + clusterUri;
            }

            clusterUri = KustoFacts.GetFullHostName(clusterUri, _defaultDomain);

            // borrow most security settings from default cluster connection
            var connection = new KustoConnectionStringBuilder(_defaultConnection);
            connection.DataSource = clusterUri;
            connection.ApplicationCertificateBlob = _defaultConnection.ApplicationCertificateBlob;
            connection.ApplicationKey = _defaultConnection.ApplicationKey;
            connection.InitialCatalog = "NetDefaultDB";

            return connection;
        }

        public class ShowDatabasesResult
        {
            public string DatabaseName;
            public string PersistentStorage;
            public string Version;
            public bool IsCurrent;
            public string DatabaseAccessMode;
            public string PrettyName;
            public bool ReservedSlot1;
            public Guid DatabaseId;
            public string InTransitionTo;
        }

        public class ShowDatabaseSchemaResult
        {
            public string DatabaseName;
            public string TableName;
            public string ColumnName;
            public string ColumnType;
            public bool IsDefaultTable;
            public bool IsDefaultColumn;
            public string PrettyName;
            public string Version;
            public string Folder;
            public string DocString;
        }

        public class ShowExternalTablesResult
        {
            public string TableName;
            public string DocString;
        }

        public class ShowExternalTableSchemaResult
        {
            public string TableName;
            public string Schema;
        }

        public class ShowMaterializedViewsResult
        {
            public string Name;
            public string DocString;
            public string Query;
        }

        public class ShowMaterializedViewSchemaResult
        {
            public string Name;
            public string Schema;
        }

        public class ShowFunctionsResult
        {
            public string Name;
            public string Parameters;
            public string Body;
            public string Folder;
            public string DocString;
        }

        public class ShowEntityGroupsResult
        {
            public string Name;
            public string Entities;
        }
    }
}
