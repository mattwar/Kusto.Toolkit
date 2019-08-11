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
        private readonly string _defaultConnection;
        private readonly HashSet<string> _badClusterNames = new HashSet<string>();

        /// <summary>
        /// Creates a new <see cref="SymbolLoader"/> instance.
        /// </summary>
        /// <param name="clusterConnection">The cluster connection string.</param>
        public SymbolLoader(string clusterConnection)
        {
            this._defaultConnection = clusterConnection;
        }

        /// <summary>
        /// Gets a list of all the database names in the cluster associated with the connection.
        /// </summary>
        public async Task<string[]> GetDatabaseNamesAsync(string clusterName = null, bool throwOnError = false, CancellationToken cancellationToken = default)
        {
            var connection = GetClusterConnection(clusterName);
            var databases = await ExecuteControlCommandAsync<ShowDatabasesResult>(connection, "", ".show databases", throwOnError, cancellationToken);
            if (databases == null)
                return null;

            return databases.Select(d => d.DatabaseName).ToArray();
        }

        /// <summary>
        /// Loads the schema for the specified databasea into a a <see cref="DatabaseSymbol"/>.
        /// </summary>
        public async Task<DatabaseSymbol> LoadDatabaseAsync(string databaseName, string clusterName = null, bool throwOnError = false, CancellationToken cancellationToken = default)
        {
            var members = new List<Symbol>();

            var connection = GetClusterConnection(clusterName);

            var tableSchemas = await ExecuteControlCommandAsync<ShowDatabaseSchemaResult>(connection, databaseName, $".show database {databaseName} schema", throwOnError, cancellationToken).ConfigureAwait(false);
            if (tableSchemas == null)
                return null;

            tableSchemas = tableSchemas
                .Where(r => !string.IsNullOrEmpty(r.TableName) && !string.IsNullOrEmpty(r.ColumnName))
                .ToArray();

            foreach (var table in tableSchemas.GroupBy(s => s.TableName))
            {
                var columns = table.Select(s => new ColumnSymbol(s.ColumnName, GetKustoType(s.ColumnType))).ToList();
                var tableSymbol = new TableSymbol(table.Key, columns);
                members.Add(tableSymbol);
            }

            var functionSchemas = await ExecuteControlCommandAsync<ShowFunctionsResult>(connection, databaseName, ".show functions", throwOnError, cancellationToken).ConfigureAwait(false);
            if (functionSchemas == null)
                return null;

            foreach (var fun in functionSchemas)
            {
                var parameters = TranslateParameters(fun.Parameters);
                var functionSymbol = new FunctionSymbol(fun.Name, fun.Body, parameters);
                members.Add(functionSymbol);
            }

            var databaseSymbol = new DatabaseSymbol(databaseName, members);
            return databaseSymbol;
        }

        /// <summary>
        /// Loads the schema for the specified database and returns a new <see cref="GlobalState"/> with the database added or updated.
        /// </summary>
        public async Task<GlobalState> AddOrUpdateDatabaseAsync(GlobalState globals, string databaseName, string clusterName = null, bool asDefault = false, bool throwOnError = false, CancellationToken cancellation = default)
        {
            var db = await LoadDatabaseAsync(databaseName, clusterName, throwOnError, cancellation).ConfigureAwait(false);
            if (db == null)
                return globals;

            var clusterHost = GetClusterHost(clusterName);

            var cluster = globals.GetCluster(clusterHost);
            if (cluster == null)
            {
                cluster = new ClusterSymbol(clusterHost, new[] { db }, isOpen: true);
                globals = globals.AddOrUpdateCluster(cluster);
            }
            else
            {
                cluster = cluster.AddOrUpdateDatabase(db);
                globals = globals.AddOrUpdateCluster(cluster);
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
        public async Task<GlobalState> AddReferencedDatabasesAsync(GlobalState globals, string query, CancellationToken cancellationToken = default)
        {
            var code = await Task.Run(() => KustoCode.ParseAndAnalyze(query, globals, cancellationToken));
            return await AddReferencedDatabasesAsync(code, cancellationToken).ConfigureAwait(false);
        }

        /// <summary>
        /// Loads and adds the <see cref="DatabaseSymbol"/> for any database explicity referenced in the query but not already present in the <see cref="GlobalState"/>.
        /// </summary>
        public async Task<GlobalState> AddReferencedDatabasesAsync(KustoCode code, CancellationToken cancellationToken = default)
        {
            var services = new KustoCodeService(code);
            var globals = code.Globals;

            // find all explicit cluster (xxx) references
            var clusterRefs = services.GetClusterReferences(cancellationToken);
            foreach (ClusterReference clusterRef in clusterRefs)
            {
                // don't bother with cluster names that we've already shown to not exist
                if (_badClusterNames.Contains(clusterRef.Cluster))
                    continue;

                var cluster = globals.GetCluster(clusterRef.Cluster);
                if (cluster == null || cluster.IsOpen)
                {
                    // check to see if this is an actual cluster and get all database names
                    var databaseNames = await GetDatabaseNamesAsync(clusterRef.Cluster).ConfigureAwait(false);
                    if (databaseNames != null)
                    {
                        // initially populate with empty 'open' databases. These will get updated to full schema if referenced
                        var databases = databaseNames.Select(db => new DatabaseSymbol(db, null, isOpen: true)).ToArray();
                        cluster = new ClusterSymbol(clusterRef.Cluster, databases);
                        globals = globals.AddOrUpdateCluster(cluster);
                    }
                }
                else
                {
                    _badClusterNames.Add(clusterRef.Cluster);
                }
            }

            // examine all explicit database(xxx) references
            var dbRefs = services.GetDatabaseReferences(cancellationToken);
            foreach (DatabaseReference dbRef in dbRefs)
            {
                // get implicit or explicit named cluster
                var cluster = string.IsNullOrEmpty(dbRef.Cluster) ? globals.Cluster : globals.GetCluster(dbRef.Cluster);

                if (cluster != null)
                {
                    // look for existing database of this name
                    var db = (DatabaseSymbol)cluster.Members.FirstOrDefault(m => m.Name == dbRef.Database);

                    // is this one of those not-yet-populated databases?
                    if (db == null || (db != null && db.Members.Count == 0 && db.IsOpen))
                    {
                        var newGlobals = await AddOrUpdateDatabaseAsync(globals, dbRef.Database, cluster.Name, asDefault: false, throwOnError: false, cancellationToken).ConfigureAwait(false);
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
                case "System.Int32":
                case "Int32":
                case "int":
                    return ScalarTypes.Int;
                case "System.Int64":
                case "Int64":
                case "long":
                    return ScalarTypes.Long;
                case "System.Double":
                case "Double":
                case "double":
                case "float":
                    return ScalarTypes.Real;
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
            var variable = let.Name.ReferencedSymbol as VariableSymbol;
            var function = variable.Type as FunctionSymbol;
            return function.Signatures[0].Parameters;
        }

        /// <summary>
        /// Executes a query or command against a kusto cluster and returns a sequence of result row instances.
        /// </summary>
        private static async Task<IEnumerable<T>> ExecuteControlCommandAsync<T>(string connection, string database, string command, bool throwOnError, CancellationToken cancellationToken)
        {
            try
            {
                using (var client = KustoClientFactory.CreateCslAdminProvider(connection))
                {
                    var resultReader = await client.ExecuteControlCommandAsync(database, command).ConfigureAwait(false);
                    var results = KustoDataReaderParser.ParseV1(resultReader, null);
                    var tableReader = results[WellKnownDataSet.PrimaryResult].Single().TableData.CreateDataReader();
                    var objectReader = new ObjectReader<T>(tableReader);
                    return objectReader;
                }
            }
            catch (Exception) when (!throwOnError)
            {
                return null;
            }
        }

        private string GetClusterHost(string clusterName)
        {
            var connection = GetClusterConnection(clusterName);
            var csb = new KustoConnectionStringBuilder(connection);
            var uri = new Uri(csb.DataSource);
            return uri.Host;
        }

        private string GetClusterConnection(string clusterName)
        {
            if (!string.IsNullOrEmpty(clusterName))
            {
                var csb = new KustoConnectionStringBuilder(_defaultConnection);
                var defaultUri = new Uri(csb.DataSource);

                if (Uri.TryCreate(clusterName, UriKind.Absolute, out var clusterUri))
                {
                    if (string.Compare(clusterUri.Host, defaultUri.Host, ignoreCase: true) != 0)
                    {
                        csb.DataSource = clusterName;
                        return csb.ConnectionString;
                    }
                }
                else
                {
                    var host = clusterName;

                    if (!host.Contains('.'))
                    {
                        host += ".kusto.windows.net";
                    }

                    if (string.Compare(host, defaultUri.Host, ignoreCase: true) != 0)
                    {
                        var scheme = clusterUri.Scheme;
                        var port = clusterUri.Port != 0 ? clusterUri.Port : 0;
                        csb.DataSource = scheme + "://" + host + (port != 0 ? ":" + port : "");
                        return csb.ConnectionString;
                    }
                }
            }

            return _defaultConnection;
        }

        private string GetDatabaseConnection(string connection, string databaseName)
        {
            var csb = new KustoConnectionStringBuilder(connection);
            csb.InitialCatalog = databaseName;
            return csb.ConnectionString;
        }

        public class ShowDatabasesResult
        {
            public string DatabaseName;
            public string PersistentStorage;
            public string Version;
            public bool IsCurrent;
            public string DatabaseAccessMode;
            public string PrettyName;
            public bool CurrentUserIsUnrestrictedViewer;
            public string DatabaseId;
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
            public string DocName;
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