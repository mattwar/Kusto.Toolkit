using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Kusto.Cloud.Platform.Data;
using Kusto.Data;
using Kusto.Data.Data;
using Kusto.Data.Net.Client;
using Kusto.Language;
using Kusto.Language.Editor;
using Kusto.Language.Syntax;
using Kusto.Language.Symbols;

namespace Kushy
{
    /// <summary>
    /// A class that retrieves schema information from a cluster as <see cref="Symbol"/> instances.
    /// </summary>
    public class SymbolLoader
    {
        private readonly KustoConnectionStringBuilder _defaultConnection;
        private readonly string _defaultClusterName;
        private readonly string _defaultDomain;
        private readonly HashSet<string> _ignoreClusterNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        private readonly Dictionary<string, HashSet<string>> _badDatabaseNameMap = new Dictionary<string, HashSet<string>>();

        /// <summary>
        /// Creates a new <see cref="SymbolLoader"/> instance. recommended method: SymbolLoader(KustoConnectionStringBuilder clusterConnection)
        /// </summary>
        /// <param name="clusterConnection">The cluster connection string.</param>
        /// <param name="defaultDomain">The domain used to convert short cluster host names into full cluster host names.
        /// This string must start with a dot.  If not specified, the default domain is ".Kusto.Windows.Net"
        /// </param>
        public SymbolLoader(string clusterConnection, string defaultDomain = null)
            : this(new KustoConnectionStringBuilder(clusterConnection), defaultDomain)
        {
        }

        /// <summary>
        /// Creates a new <see cref="SymbolLoader"/> instance.
        /// </summary>
        /// <param name="clusterConnection">The cluster connection.</param>
        /// <param name="defaultDomain">The domain used to convert short cluster host names into full cluster host names.
        /// This string must start with a dot.  If not specified, the default domain is ".Kusto.Windows.Net"
        /// </param>
        public SymbolLoader(KustoConnectionStringBuilder clusterConnection, string defaultDomain = null)
        {
            _defaultConnection = clusterConnection;
            _defaultClusterName = GetHost(clusterConnection);
            _defaultDomain = String.IsNullOrEmpty(defaultDomain) 
                ? KustoFacts.KustoWindowsNet
                : defaultDomain;
        }

        /// <summary>
        /// Gets a list of all the database names in the cluster associated with the connection.
        /// </summary>
        public async Task<string[]> GetDatabaseNamesAsync(string clusterName = null, bool throwOnError = false, CancellationToken cancellationToken = default)
        {
            clusterName = string.IsNullOrEmpty(clusterName)
                ? _defaultClusterName
                : GetFullHostName(clusterName);

            var connection = GetClusterConnection(clusterName);
            var databases = await ExecuteControlCommandAsync<ShowDatabasesResult>(connection, "", ".show databases", throwOnError, cancellationToken);
            if (databases == null)
                return null;

            return databases.Select(d => d.DatabaseName).ToArray();
        }

        /// <summary>
        /// Loads the schema for the specified database into a <see cref="DatabaseSymbol"/>.
        /// </summary>
        public async Task<DatabaseSymbol> LoadDatabaseAsync(string databaseName, string clusterName = null, bool throwOnError = false, CancellationToken cancellationToken = default)
        {
            clusterName = string.IsNullOrEmpty(clusterName)
                ? _defaultClusterName
                : GetFullHostName(clusterName);

            // if we've already determined this database name is bad, then bail out
            if (_badDatabaseNameMap.TryGetValue(clusterName, out var badDbNames)
                && badDbNames.Contains(databaseName))
                return null;

            var connection = GetClusterConnection(clusterName);

            var tables = await LoadTablesAsync(connection, databaseName, throwOnError, cancellationToken).ConfigureAwait(false);
            if (tables == null)
            {
                if (badDbNames == null)
                {
                    badDbNames = new HashSet<string>();
                    _badDatabaseNameMap.Add(clusterName, badDbNames);
                }
                badDbNames.Add(databaseName);
                return null;
            }

            var externalTables = await LoadExternalTablesAsync(connection, databaseName, throwOnError, cancellationToken).ConfigureAwait(false);
            var materializedViews = await LoadMaterializedViewsAsync(connection, databaseName, throwOnError, cancellationToken).ConfigureAwait(false);
            var functions = await LoadFunctionsAsync(connection, databaseName, throwOnError, cancellationToken).ConfigureAwait(false);

            var members = new List<Symbol>();
            members.AddRange(tables);
            members.AddRange(externalTables);
            members.AddRange(materializedViews);
            members.AddRange(functions);

            var databaseSymbol = new DatabaseSymbol(databaseName, members);
            return databaseSymbol;
        }

        private async Task<IReadOnlyList<TableSymbol>> LoadTablesAsync(KustoConnectionStringBuilder connection, string databaseName, bool throwOnError, CancellationToken cancellationToken)
        {
            var tables = new List<TableSymbol>();

            // get table schema from .show database xxx schema
            var databaseSchemas = await ExecuteControlCommandAsync<ShowDatabaseSchemaResult>(connection, databaseName, $".show database {databaseName} schema", throwOnError, cancellationToken).ConfigureAwait(false);
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

        private async Task<IReadOnlyList<TableSymbol>> LoadExternalTablesAsync(KustoConnectionStringBuilder connection, string databaseName, bool throwOnError, CancellationToken cancellationToken)
        {
            var tables = new List<TableSymbol>();

            // get external tables from .show external tables and .show external table xxx cslschema
            var externalTables = await ExecuteControlCommandAsync<ShowExternalTablesResult>(connection, databaseName, ".show external tables", throwOnError, cancellationToken);
            if (externalTables != null)
            {
                foreach (var et in externalTables)
                {
                    var etSchemas = await ExecuteControlCommandAsync<ShowExternalTableSchemaResult>(connection, databaseName, $".show external table {et.TableName} cslschema", throwOnError, cancellationToken);
                    if (etSchemas != null && etSchemas.Length > 0)
                    {
                        var mvSymbol = new TableSymbol(et.TableName, "(" + etSchemas[0].Schema + ")", et.DocString).WithIsExternal(true);
                        tables.Add(mvSymbol);
                    }
                }
            }

            return tables;
        }

        private async Task<IReadOnlyList<TableSymbol>> LoadMaterializedViewsAsync(KustoConnectionStringBuilder connection, string databaseName, bool throwOnError, CancellationToken cancellationToken)
        {
            var tables = new List<TableSymbol>();

            // get materialized views from .show materialized-views and .show materialized-view xxx cslschema
            var materializedViews = await ExecuteControlCommandAsync<ShowMaterializedViewsResult>(connection, databaseName, ".show materialized-views", throwOnError, cancellationToken);
            if (materializedViews != null)
            {
                foreach (var mv in materializedViews)
                {
                    var mvSchemas = await ExecuteControlCommandAsync<ShowMaterializedViewSchemaResult>(connection, databaseName, $".show materialized-view {mv.Name} cslschema", throwOnError, cancellationToken);
                    if (mvSchemas != null && mvSchemas.Length > 0)
                    {
                        var mvSymbol = new TableSymbol(mv.Name, "(" + mvSchemas[0].Schema + ")", mv.DocString).WithIsMaterializedView(true);
                        tables.Add(mvSymbol);
                    }
                }
            }

            return tables;
        }

        private async Task<IReadOnlyList<FunctionSymbol>> LoadFunctionsAsync(KustoConnectionStringBuilder connection, string databaseName, bool throwOnError, CancellationToken cancellationToken)
        {
            var functions = new List<FunctionSymbol>();

            // get functions for .show functions
            var functionSchemas = await ExecuteControlCommandAsync<ShowFunctionsResult>(connection, databaseName, ".show functions", throwOnError, cancellationToken).ConfigureAwait(false);
            if (functionSchemas == null)
                return null;

            foreach (var fun in functionSchemas)
            {
                var parameters = TranslateParameters(fun.Parameters);
                var functionSymbol = new FunctionSymbol(fun.Name, fun.Body, parameters);
                functions.Add(functionSymbol);
            }

            return functions;
        }

        /// <summary>
        /// Loads the schema for the specified database and returns a new <see cref="GlobalState"/> with the database added or updated.
        /// </summary>
        public Task<GlobalState> AddOrUpdateDatabaseAsync(GlobalState globals, string databaseName, string clusterName = null, bool throwOnError = false, CancellationToken cancellation = default)
        {
            return AddOrUpdateDatabaseAsync(globals, databaseName, clusterName, asDefault: false, throwOnError, cancellation);
        }

        /// <summary>
        /// Loads the schema for the specified default database and returns a new <see cref="GlobalState"/> with the database added or updated.
        /// </summary>
        public Task<GlobalState> AddOrUpdateDefaultDatabaseAsync(GlobalState globals, string databaseName, string clusterName = null, bool throwOnError = false, CancellationToken cancellation = default)
        {
            return AddOrUpdateDatabaseAsync(globals, databaseName, clusterName, asDefault: true, throwOnError, cancellation);
        }

        /// <summary>
        /// Loads the schema for the specified database and returns a new <see cref="GlobalState"/> with the database added or updated.
        /// </summary>
        private async Task<GlobalState> AddOrUpdateDatabaseAsync(GlobalState globals, string databaseName, string clusterName, bool asDefault, bool throwOnError, CancellationToken cancellation)
        {
            clusterName = string.IsNullOrEmpty(clusterName)
                ? _defaultClusterName
                : GetFullHostName(clusterName);

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
        /// Loads and adds the <see cref="DatabaseSymbol"/> for any database explicity referenced in the query but not already present in the <see cref="GlobalState"/>.
        /// </summary>
        public async Task<KustoCode> AddReferencedDatabasesAsync(KustoCode code, bool throwOnError = false, CancellationToken cancellationToken = default)
        {
            var service = new KustoCodeService(code);
            var globals = await AddReferencedDatabasesAsync(code.Globals, service, throwOnError, cancellationToken).ConfigureAwait(false);
            return code.WithGlobals(globals);
        }

        /// <summary>
        /// Loads and adds the <see cref="DatabaseSymbol"/> for any database explicity referenced in the <see cref="CodeScript"/ document but not already present in the <see cref="GlobalState"/>.
        /// </summary>
        public async Task<CodeScript> AddReferencedDatabasesAsync(CodeScript script, bool throwOnError = false, CancellationToken cancellationToken = default)
        {
            var globals = script.Globals;

            foreach (var block in script.Blocks)
            {
                globals = await AddReferencedDatabasesAsync(globals, block.Service, throwOnError, cancellationToken).ConfigureAwait(false);
            }

            return script.WithGlobals(globals);
        }

        /// <summary>
        /// Loads and adds the <see cref="DatabaseSymbol"/> for any database explicity referenced in the query but not already present in the <see cref="GlobalState"/>.
        /// </summary>
        private async Task<GlobalState> AddReferencedDatabasesAsync(GlobalState globals, CodeService service, bool throwOnError = false,CancellationToken cancellationToken = default)
        {
            // find all explicit cluster (xxx) references
            var clusterRefs = service.GetClusterReferences(cancellationToken);
            foreach (ClusterReference clusterRef in clusterRefs)
            {
                var clusterName = GetFullHostName(clusterRef.Cluster);

                // don't bother with cluster names that we've already shown to not exist
                if (_ignoreClusterNames.Contains(clusterName))
                    continue;

                var cluster = globals.GetCluster(clusterName);
                if (cluster == null || cluster.IsOpen)
                {
                    // check to see if this is an actual cluster and get all database names
                    var databaseNames = await GetDatabaseNamesAsync(clusterName, throwOnError).ConfigureAwait(false);
                    if (databaseNames != null)
                    {
                        // initially populate with empty 'open' databases. These will get updated to full schema if referenced
                        var databases = databaseNames.Select(db => new DatabaseSymbol(db, null, isOpen: true)).ToArray();
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
                    : GetFullHostName(dbRef.Cluster);

                // get implicit or explicit named cluster
                var cluster = string.IsNullOrEmpty(clusterName) 
                    ? globals.Cluster 
                    : globals.GetCluster(clusterName);

                if (cluster != null)
                {
                    // look for existing database of this name
                    var db = cluster.Databases.FirstOrDefault(m => m.Name.Equals(dbRef.Database, StringComparison.OrdinalIgnoreCase));

                    // is this one of those not-yet-populated databases?
                    if (db == null || (db != null && db.Members.Count == 0 && db.IsOpen))
                    {
                        var newGlobals = await AddOrUpdateDatabaseAsync(globals, dbRef.Database, clusterName, asDefault: false, throwOnError, cancellationToken).ConfigureAwait(false);
                        globals = newGlobals != null ? newGlobals : globals;
                    }
                }
            }

            return globals;
        }

        /// <summary>
        /// Convert CLR type name into a Kusto scalar type.
        /// </summary>
        private static ScalarSymbol GetKustoType(string clrTypeName)
        {
            switch (clrTypeName)
            {
                case "System.Byte":
                case "Byte":
                case "byte":
                case "System.SByte":
                case "SByte":
                case "sbyte":
                case "System.Int16":
                case "Int16":
                case "short":
                case "System.UInt16":
                case "UInt16":
                case "ushort":
                case "System.Int32":
                case "Int32":
                case "int":
                    return ScalarTypes.Int;
                case "System.UInt32": // unsigned ints don't fit into int, use long
                case "UInt32":
                case "uint":
                case "System.Int64":
                case "Int64":
                case "long":
                    return ScalarTypes.Long;
                case "System.Double":
                case "Double":
                case "double":
                case "float":
                case "System.single":
                case "System.Single":
                    return ScalarTypes.Real;
                case "System.UInt64": // unsigned longs do not fit into long, use decimal
                case "UInt64":
                case "ulong":
                case "System.Decimal":
                case "Decimal":
                case "decimal":
                case "System.Data.SqlTypes.SqlDecimal":
                case "SqlDecimal":
                    return ScalarTypes.Decimal;
                case "System.Guid":
                case "Guid":
                    return ScalarTypes.Guid;
                case "System.DateTime":
                case "DateTime":
                    return ScalarTypes.DateTime;
                case "System.TimeSpan":
                case "TimeSpan":
                    return ScalarTypes.TimeSpan;
                case "System.String":
                case "String":
                case "string":
                    return ScalarTypes.String;
                case "System.Boolean":
                case "Boolean":
                case "bool":
                    return ScalarTypes.Bool;
                case "System.Object":
                case "Object":
                case "object":
                    return ScalarTypes.Dynamic;
                case "System.Type":
                case "Type":
                    return ScalarTypes.Type;
                default:
                    throw new InvalidOperationException($"Unhandled clr type: {clrTypeName}");
            }
        }

        private static IReadOnlyList<Parameter> NoParameters = new Parameter[0];

        /// <summary>
        /// Translate Kusto parameter list declaration into into list of <see cref="Parameter"/> instances.
        /// </summary>
        private static IReadOnlyList<Parameter> TranslateParameters(string parameters)
        {
            parameters = parameters.Trim();

            if (string.IsNullOrEmpty(parameters) || parameters == "()")
                return NoParameters;

            if (parameters[0] != '(')
                parameters = "(" + parameters;
            if (parameters[parameters.Length - 1] != ')')
                parameters = parameters + ")";

            var query = "let fn = " + parameters + " { };";
            var code = KustoCode.ParseAndAnalyze(query);
            var let = code.Syntax.GetFirstDescendant<LetStatement>();

            if (let.Name.ReferencedSymbol is FunctionSymbol fs)
            {
                return fs.Signatures[0].Parameters;
            }
            else if (let.Name.ReferencedSymbol is VariableSymbol vs
                && vs.Type is FunctionSymbol vfs)
            {
                return vfs.Signatures[0].Parameters;
            }
            else
            {
                return NoParameters;
            }
        }

        /// <summary>
        /// Executes a query or command against a kusto cluster and returns a sequence of result row instances.
        /// </summary>
        private static async Task<T[]> ExecuteControlCommandAsync<T>(KustoConnectionStringBuilder connection, string database, string command, bool throwOnError, CancellationToken cancellationToken)
        {
            try
            {
                using (var client = KustoClientFactory.CreateCslAdminProvider(connection))
                {
                    var resultReader = await client.ExecuteControlCommandAsync(database, command).ConfigureAwait(false);
                    var results = KustoDataReaderParser.ParseV1(resultReader, null);
                    var tableReader = results[WellKnownDataSet.PrimaryResult].Single().TableData.CreateDataReader();
                    var objectReader = new ObjectReader<T>(tableReader);
                    return objectReader.ToArray();
                }
            }
            catch (Exception) when (!throwOnError)
            {
                return null;
            }
        }

        private string GetFullHostName(string clusterNameOrUri)
        {
            return KustoFacts.GetFullHostName(KustoFacts.GetHostName(clusterNameOrUri), _defaultDomain);
        }

        private string GetClusterHost(string clusterName)
        {
            return GetHost(GetClusterConnection(clusterName));
        }

        private string GetHost(KustoConnectionStringBuilder connection)
        {
            //var csb = new KustoConnectionStringBuilder(connection);
            var uri = new Uri(connection.DataSource);
            return uri.Host;
        }

        private KustoConnectionStringBuilder GetClusterConnection(string clusterUriOrName)
        {
            if (string.IsNullOrEmpty(clusterUriOrName)
                || clusterUriOrName == _defaultClusterName)
            {
                return _defaultConnection;
            }

            // borrow most security settings from default cluster connection
            var builder = new KustoConnectionStringBuilder(_defaultConnection);

            if (string.IsNullOrWhiteSpace(clusterUriOrName))
                return null;

            var clusterUri = clusterUriOrName;

            clusterUri = KustoFacts.GetFullHostName(clusterUri, _defaultDomain);

            if (!clusterUri.Contains("://"))
                clusterUri = builder.ConnectionScheme + "://" + clusterUri;

            builder.DataSource = clusterUri;
            builder.ApplicationCertificateBlob = _defaultConnection.ApplicationCertificateBlob;
            builder.ApplicationKey = _defaultConnection.ApplicationKey;
            builder.InitialCatalog = "NetDefaultDB";

            return builder;
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
    }
}