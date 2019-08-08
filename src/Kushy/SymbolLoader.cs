using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using Kusto.Cloud.Platform.Data;
using Kusto.Data;
using Kusto.Data.Common;
using Kusto.Data.Data;
using Kusto.Data.Net.Client;
using Kusto.Language.Syntax;
using Kusto.Language.Symbols;

namespace Kusto.Language
{
    /// <summary>
    /// A class that retrieves schema information from a cluster as <see cref="Symbol"/> instances.
    /// </summary>
    public class SymbolLoader
    {
        private readonly string _connection;

        /// <summary>
        /// Creates a new <see cref="SymbolLoader"/> instance.
        /// </summary>
        /// <param name="clusterConnection">The cluster connection string.</param>
        public SymbolLoader(string clusterConnection)
        {
            this._connection = clusterConnection;
        }

        /// <summary>
        /// Gets a list of all the database names in the cluster associated with the connection.
        /// </summary>
        public async Task<string[]> GetDatabaseNamesAsync(CancellationToken cancellationToken = default)
        {
            var databases = await ExecuteControlCommandAsync<ShowDatabasesResult>(_connection, "", ".show databases", cancellationToken);
            return databases.Select(d => d.DatabaseName).ToArray();
        }

        /// <summary>
        /// Gets a <see cref="DatabaseSymbol"/> instance matching the schema of a database.
        /// </summary>
        public async Task<DatabaseSymbol> GetDatabaseSymbolAsync(string databaseName, CancellationToken cancellationToken = default)
        {
            var members = new List<Symbol>();

            var csb = new KustoConnectionStringBuilder(_connection);
            csb.InitialCatalog = databaseName;
            var connectionWithDatabase = csb.ConnectionString;

            var tableSchemas = 
                (await ExecuteControlCommandAsync<ShowDatabaseSchemaResult>(connectionWithDatabase, databaseName, $".show database {databaseName} schema", cancellationToken).ConfigureAwait(false))
                .Where(r => !string.IsNullOrEmpty(r.TableName) && !string.IsNullOrEmpty(r.ColumnName))
                .ToArray();

            foreach (var table in tableSchemas.GroupBy(s => s.TableName))
            {
                var columns = table.Select(s => new ColumnSymbol(s.ColumnName, GetKustoType(s.ColumnType))).ToList();
                var tableSymbol = new TableSymbol(table.Key, columns);
                members.Add(tableSymbol);
            }

            var functionSchemas =
                (await ExecuteControlCommandAsync<ShowFunctionsResult>(connectionWithDatabase, databaseName, ".show functions", cancellationToken).ConfigureAwait(false))
                .ToArray();

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
        private static async Task<IEnumerable<T>> ExecuteControlCommandAsync<T>(string connection, string database, string command, CancellationToken cancellationToken)
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