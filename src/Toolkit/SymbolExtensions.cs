using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using Kusto.Language;
using Kusto.Language.Symbols;

namespace Kusto.Toolkit
{
    public static class SymbolExtensions
    {
        /// <summary>
        /// Adds or updates members of the <see cref="DatabaseSymbol"/>.
        /// </summary>
        public static ClusterSymbol AddOrUpdateDatabases(this ClusterSymbol cluster, IEnumerable<DatabaseSymbol> newDatabases)
        {
            var databaseList = cluster.Databases.ToList();
            var changed = false;

            foreach (var newDb in newDatabases)
            {
                var existingDb = cluster.GetDatabase(newDb.Name);
                if (existingDb == null)
                {
                    databaseList.Add(newDb);
                    changed = true;
                }
                else
                {
                    var index = databaseList.IndexOf(existingDb);
                    if (index >= 0)
                    {
                        databaseList[index] = newDb;
                        changed = true;
                    }
                }
            }

            return changed ? cluster.WithDatabases(databaseList) : cluster;
        }

        /// <summary>
        /// Adds or updates members of the <see cref="DatabaseSymbol"/>.
        /// </summary>
        public static ClusterSymbol AddOrUpdateDatabases(this ClusterSymbol cluster, params DatabaseSymbol[] newDatabases)
        {
            return cluster.AddOrUpdateDatabases((IReadOnlyList<DatabaseSymbol>)newDatabases);
        }

        /// <summary>
        /// Adds or updates members of the <see cref="DatabaseSymbol"/>
        /// </summary>
        public static DatabaseSymbol AddOrUpdateMembers(this DatabaseSymbol database, params Symbol[] newMembers)
        {
            return AddOrUpdateMembers(database, (IEnumerable<Symbol>)newMembers);
        }

        /// <summary>
        /// Adds or updates members of the <see cref="DatabaseSymbol"/>.
        /// </summary>
        public static DatabaseSymbol AddOrUpdateMembers(this DatabaseSymbol database, IEnumerable<Symbol> newMembers)
        {
            if (newMembers.Count() == 1
                && database.GetMember(newMembers.ElementAt(0).Name) == null)
            {
                return database.AddMembers(newMembers);
            }

            var memberList = database.Members.ToList();
            var changed = false;

            foreach (var newMember in newMembers)
            {
                var existingMember = database.GetMember(newMember.Name);
                if (existingMember == null)
                {
                    memberList.Add(newMember);
                    changed = true;
                }
                else
                {
                    var index = memberList.IndexOf(existingMember);
                    if (index >= 0)
                    {
                        memberList[index] = newMember;
                        changed = true;
                    }
                }
            }

            return changed ? database.WithMembers(memberList) : database;
        }

        /// <summary>
        /// Removes members from the database symbol
        /// </summary>
        public static DatabaseSymbol RemoveMembers(this DatabaseSymbol database, IEnumerable<Symbol> members)
        {
            var newMemberList = database.Members.Except(members).ToList();
            return database.WithMembers(newMemberList);
        }

        /// <summary>
        /// Removes members from the database symbol
        /// </summary>
        public static DatabaseSymbol RemoveMembers(this DatabaseSymbol database, params Symbol[] members)
        {
            return RemoveMembers(database, (IEnumerable<Symbol>)members);
        }

        /// <summary>
        /// Replaces one or more existing members with alternate members.
        /// </summary>
        public static DatabaseSymbol ReplaceMembers<TMember>(this DatabaseSymbol database, IEnumerable<(TMember original, TMember replacement)> replacements)
            where TMember : Symbol
        {
            var list = database.Members.ToList();
            var changed = false;

            foreach (var pair in replacements)
            {
                var index = list.IndexOf(pair.original);
                if (index >= 0)
                {
                    list[index] = pair.replacement;
                    changed = true;
                }
            }

            return changed
                ? database.WithMembers(list)
                : database;
        }

        /// <summary>
        /// Replaces one or more existing database members with alternate members.
        /// </summary>
        public static DatabaseSymbol ReplaceMembers<TMember>(this DatabaseSymbol database, params (TMember original, TMember replacement)[] replacements)
            where TMember : Symbol
        {
            return database.ReplaceMembers((IEnumerable<(TMember original, TMember replacement)>)replacements);
        }

        /// <summary>
        /// Adds or updates columns of the <see cref="TableSymbol"/>.
        /// </summary>
        public static TableSymbol AddOrUpdateColumns(this TableSymbol table, IEnumerable<ColumnSymbol> newColumns)
        {
            var columnList = table.Columns.ToList();
            var changed = false;

            foreach (var newColumn in newColumns)
            {
                var existingColumn = columnList.FirstOrDefault(c => c.Name == newColumn.Name);
                if (existingColumn == null)
                {
                    columnList.Add(newColumn);
                    changed = true;
                }
                else
                {
                    var index = columnList.IndexOf(existingColumn);
                    if (index >= 0)
                    {
                        columnList[index] = newColumn;
                        changed = true;
                    }
                }
            }

            return changed
                ? table.WithColumns(columnList)
                : table;
        }

        /// <summary>
        /// Adds or updates columns of the <see cref="TableSymbol"/>.
        /// </summary>
        public static TableSymbol AddOrUpdateColumns(this TableSymbol table, params ColumnSymbol[] newColumns)
        {
            return table.AddOrUpdateColumns((IEnumerable<ColumnSymbol>)newColumns);
        }

        /// <summary>
        /// Removes the specified columns from the table.
        /// </summary>
        public static TableSymbol RemoveColumns(this TableSymbol table, IEnumerable<ColumnSymbol> columns)
        {
            return table.WithColumns(table.Columns.Except(columns));
        }

        /// <summary>
        /// Removes the specified columns from the table.
        /// </summary>
        public static TableSymbol RemoveColumns(this TableSymbol table, params ColumnSymbol[] columns)
        {
            return table.RemoveColumns((IEnumerable<ColumnSymbol>)columns);
        }

        /// <summary>
        /// Replaces one or more existing columns with alternate columns.
        /// </summary>
        public static TableSymbol ReplaceColumns(this TableSymbol table, IEnumerable<(ColumnSymbol original, ColumnSymbol replacement)> replacements)
        {
            var list = table.Columns.ToList();
            var changed = false;

            foreach (var pair in replacements)
            {
                var index = list.IndexOf(pair.original);
                if (index >= 0)
                {
                    list[index] = pair.replacement;
                    changed = true;
                }
            }

            return changed
                ? table.WithColumns(list)
                : table;
        }

        /// <summary>
        /// Replaces one or more existing columns with alternate columns.
        /// </summary>
        public static TableSymbol ReplaceColumns(this TableSymbol table, params (ColumnSymbol original, ColumnSymbol replacement)[] replacements)
        {
            return table.ReplaceColumns((IEnumerable<(ColumnSymbol original, ColumnSymbol replacement)>)replacements);
        }

        /// <summary>
        /// Create a new <see cref="GraphModelSymbol"/> with the snapshots changes.
        /// </summary>
        public static GraphModelSymbol WithSnapshots(this GraphModelSymbol graphModel, IEnumerable<string> snapshots)
        {
            return new GraphModelSymbol(
                graphModel.Name, 
                graphModel.GetEdgeQueries(),
                graphModel.GetNodeQueries(),
                snapshots
                );
        }

        /// <summary>
        /// Create a new <see cref="GraphModelSymbol"/> with the edges changed.
        /// </summary>
        public static GraphModelSymbol WithEdges(this GraphModelSymbol graphModel, IEnumerable<string> edges)
        {
            return new GraphModelSymbol(
                graphModel.Name, 
                edges, 
                graphModel.GetNodeQueries(),
                graphModel.GetSnapshotNames()
                );
        }

        /// <summary>
        /// Create a new <see cref="GraphModelSymbol"/> with the nodes changed.
        /// </summary>
        public static GraphModelSymbol WithNodes(this GraphModelSymbol graphModel, IEnumerable<string> nodes)
        {
            return new GraphModelSymbol(
                graphModel.Name,
                graphModel.GetEdgeQueries(),
                nodes,
                graphModel.GetSnapshotNames()
                );
        }

        /// <summary>
        /// Gets the edge query texts.
        /// </summary>
        public static IReadOnlyList<string> GetEdgeQueries(this GraphModelSymbol graphModel) =>
            graphModel.Edges.Select(e => e.Body).ToList();

        /// <summary>
        /// Gets the node query texts.
        /// </summary>
        public static IReadOnlyList<string> GetNodeQueries(this GraphModelSymbol graphModel) =>
            graphModel.Nodes.Select(n => n.Body).ToList();

        /// <summary>
        /// Gets the snapshot names.
        /// </summary>
        public static IReadOnlyList<string> GetSnapshotNames(this GraphModelSymbol graphModel) =>
            graphModel.Snapshots.Select(sn => sn.Name).ToList();


        /// <summary>
        /// Gets the <see cref="DatabaseSymbol"/> containing this <see cref="TableSymbol"/>.
        /// </summary>
        public static bool TryGetDatabase(this TableSymbol table, GlobalState globals, out DatabaseSymbol database) =>
            TryGetDatabase((Symbol)table, globals, out database);

        /// <summary>
        /// Gets the <see cref="DatabaseSymbol"/> containing this <see cref="FunctionSymbol"/>
        /// </summary>
        public static bool TryGetDatabase(this FunctionSymbol function, GlobalState globals, out DatabaseSymbol database) =>
            TryGetDatabase((Symbol)function, globals, out database);

        /// <summary>
        /// Gets the <see cref="DatabaseSymbol"/> containing this <see cref="GraphModelSymbol"/>.
        /// </summary>
        public static bool TryGetDatabase(this GraphModelSymbol graphModel, GlobalState globals, out DatabaseSymbol database) =>
            TryGetDatabase((Symbol)graphModel, globals, out database);

        /// <summary>
        /// Gets the <see cref="DatabaseSymbol"/> containing this <see cref="Symbol"/>
        /// </summary>
        public static bool TryGetDatabase(Symbol symbol, GlobalState globals, out DatabaseSymbol database)
        {
            database = globals.GetDatabase(symbol);
            return database != null;
        }

        /// <summary>
        /// Gets the <see cref="ClusterSymbol"/> containing this <see cref="DatabaseSymbol"/>
        /// </summary>
        public static bool TryGetCluster(this DatabaseSymbol database, GlobalState globals, out ClusterSymbol cluster)
        {
            cluster = globals.GetCluster(database);
            return cluster != null;
        }

        /// <summary>
        /// Gets the <see cref="ClusterSymbol"/> and <see cref="DatabaseSymbol"/> containing this <see cref="TableSymbol"/>>.
        /// </summary>
        public static bool TryGetClusterAndDatabase(this TableSymbol table, GlobalState globals, out ClusterSymbol cluster, out DatabaseSymbol database) =>
            TryGetClusterAndDatabase((Symbol)table, globals, out cluster, out database);

        /// <summary>
        /// Gets the <see cref="ClusterSymbol"/> and <see cref="DatabaseSymbol"/> corresponding this <see cref="FunctionSymbol"/>.
        /// </summary>
        public static bool TryGetClusterAndDatabase(this FunctionSymbol function, GlobalState globals, out ClusterSymbol cluster, out DatabaseSymbol database) =>
            TryGetClusterAndDatabase((Symbol)function, globals, out cluster, out database);

        /// <summary>
        /// Gets the <see cref="ClusterSymbol"/> and <see cref="DatabaseSymbol"/> corresponding this <see cref="GraphModelSymbol"/>.
        /// </summary>
        public static bool TryGetClusterAndDatabase(this GraphModelSymbol graphModel, GlobalState globals, out ClusterSymbol cluster, out DatabaseSymbol database) =>
            TryGetClusterAndDatabase((Symbol)graphModel, globals, out cluster, out database);

        /// <summary>
        /// Gets the <see cref="ClusterSymbol"/> and <see cref="DatabaseSymbol"/> corresponding this <see cref="Symbol"/>.
        /// </summary>
        public static bool TryGetClusterAndDatabase(Symbol symbol, GlobalState globals, out ClusterSymbol cluster, out DatabaseSymbol database)
        {
            database = globals.GetDatabase(symbol);
            cluster = globals.GetCluster(database);
            return database != null && cluster != null;
        }

        /// <summary>
        /// Gets the <see cref="TableSymbol"/> containing this <see cref="ColumnSymbol"/>
        /// </summary>
        public static bool TryGetTable(this ColumnSymbol column, GlobalState globals, out TableSymbol table)
        {
            table = globals.GetTable(column);
            return table != null;
        }

        /// <summary>
        /// Gets the <see cref="DatabaseSymbol"/> and <see cref="TableSymbol"/> containing this <see cref="ColumnSymbol"/>.
        /// </summary>
        public static bool TryGetDatabaseAndTable(this ColumnSymbol column, GlobalState globals, out DatabaseSymbol database, out TableSymbol table)
        {
            table = globals.GetTable(column);
            database = globals.GetDatabase(table);
            return table != null && database != null;
        }

        /// <summary>
        /// Gets the <see cref="ClusterSymbol"/>, <see cref="DatabaseSymbol"/> and <see cref="TableSymbol"/> containing this column.
        /// </summary>
        public static bool TryGetClusterDatabaseAndTable(this ColumnSymbol column, GlobalState globals, out ClusterSymbol cluster, out DatabaseSymbol database, out TableSymbol table)
        {
            table = globals.GetTable(column);
            database = globals.GetDatabase(table);
            cluster = globals.GetCluster(database);
            return table != null && database != null && cluster != null;
        }

        /// <summary>
        /// Gets the minimal KQL expression that references the symbol.
        /// </summary>
        public static string GetMinimalExpression(this Symbol symbol, GlobalState globals)
        {
            if (symbol is DatabaseSymbol database)
            {
                var cluster = globals.GetCluster(database);
                var dbExpression = $"database({KustoFacts.GetSingleQuotedStringLiteral(database.Name)})";
                return cluster == globals.Cluster
                    ? dbExpression
                    : $"{GetMinimalExpression(cluster, globals)}.{dbExpression}";
            }
            else if (symbol is ClusterSymbol cluster)
            {
                return $"cluster({KustoFacts.GetSingleQuotedStringLiteral(cluster.Name)})";
            }
            else if (symbol is TableSymbol 
                || symbol is FunctionSymbol
                || symbol is EntityGroupSymbol
                || symbol is GraphModelSymbol)
            {
                var symbolExpression = KustoFacts.BracketNameIfNecessary(symbol.Name);
                var db = globals.GetDatabase(symbol);
                return (db != null && db != globals.Database)
                    ? $"{GetMinimalExpression(db, globals)}.{symbolExpression}"
                    : symbolExpression;
            }
            else
            {
                return KustoFacts.BracketNameIfNecessary(symbol.Name);
            }
        }

        /// <summary>
        /// Gets a KQL expression that references the symbol.
        /// </summary>
        public static string GetExpression(this Symbol symbol, GlobalState globals)
        {
            if (symbol is DatabaseSymbol database)
            {
                var cluster = globals.GetCluster(database);
                var dbExpression = $"database({KustoFacts.GetSingleQuotedStringLiteral(database.Name)})";
                return cluster != null
                    ? $"{GetExpression(cluster, globals)}.{dbExpression}"
                    : dbExpression;
            }
            else if (symbol is ClusterSymbol cluster)
            {
                return $"cluster({KustoFacts.GetSingleQuotedStringLiteral(cluster.Name)})";
            }
            else if (symbol is TableSymbol
                || symbol is FunctionSymbol
                || symbol is EntityGroupSymbol
                || symbol is GraphModelSymbol)
            {
                var symbolExpression = KustoFacts.BracketNameIfNecessary(symbol.Name);
                var db = globals.GetDatabase(symbol);
                return db != null
                    ? $"{GetExpression(db, globals)}.{symbolExpression}"
                    : symbolExpression;
            }
            else
            {
                return KustoFacts.BracketNameIfNecessary(symbol.Name);
            }
        }
    }
}
