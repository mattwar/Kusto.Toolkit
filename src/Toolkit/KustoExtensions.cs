using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Xml.Linq;
using Kusto.Language;
using Kusto.Language.Symbols;
using Kusto.Language.Syntax;
using static Kusto.Data.Net.Http.OneApiError;

#nullable disable // some day...

namespace Kusto.Toolkit
{
    public static class KustoExtensions
    {
        /// <summary>
        /// Returns a list of all the database tables that are explicitly referenced in the query.
        /// </summary>
        public static IReadOnlyList<TableSymbol> GetDatabaseTablesReferenced(this KustoCode code)
        {
            var tableSet = new HashSet<TableSymbol>();
            var tableList = new List<TableSymbol>();
            GatherTables(code.Syntax);
            return tableList;

            void GatherTables(SyntaxNode root)
            {
                SyntaxElement.WalkNodes(root,
                    fnBefore: n =>
                    {
                        if (n.ReferencedSymbol is TableSymbol t
                            && code.Globals.IsDatabaseTable(t))
                        {
                            if (tableSet.Add(t))
                                tableList.Add(t);
                        }

                        if (n is Expression e
                            && e.ResultType is TableSymbol ts
                            && code.Globals.IsDatabaseTable(ts))
                        {
                            if (tableSet.Add(ts))
                                tableList.Add(ts);
                        }

                        if (n.GetCalledFunctionBody() is SyntaxNode body)
                        {
                            GatherTables(body);
                        }
                    },
                    fnDescend: n =>
                        // skip descending into function declarations since their bodies will be examined by the code above
                        !(n is FunctionDeclaration)
                    );
            }
        }

        /// <summary>
        /// Returns a list of all the database table columns explicitly referenced in the query.
        /// </summary>
        public static IReadOnlyList<ColumnSymbol> GetDatabaseTableColumnsReferenced(this KustoCode code)
        {
            return GetDatabaseTableColumnsReferenced(code.Syntax, code.Globals);
        }

        /// <summary>
        /// Returns a list of all the database table columns explicitly referenced in syntax tree.
        /// </summary>
        public static IReadOnlyList<ColumnSymbol> GetDatabaseTableColumnsReferenced(SyntaxNode root, GlobalState globals)
        {
            var columnSet = new HashSet<ColumnSymbol>();
            var columnList = new List<ColumnSymbol>();

            GatherColumns(root);

            return columnList;

            void GatherColumns(SyntaxNode root)
            {
                SyntaxElement.WalkNodes(root,
                    fnBefore: n =>
                    {
                        if (n.ReferencedSymbol is ColumnSymbol c)
                        {
                            AddDatabaseTableColumn(c, columnSet, columnList, globals);
                        }

                        if (n.GetCalledFunctionBody() is SyntaxNode body)
                        {
                            GatherColumns(body);
                        }
                    },
                    fnDescend: n =>
                        // skip descending into function declarations since their bodies will be examined by the code above
                        !(n is FunctionDeclaration)
                    );
            }
        }

        private static void AddDatabaseTableColumn(ColumnSymbol column, HashSet<ColumnSymbol> columnSet, List<ColumnSymbol> columnList, GlobalState globals)
        {
            if (globals.GetTable(column) != null)
            {
                if (columnSet.Add(column))
                    columnList.Add(column);
            }
            else if (column.OriginalColumns.Count > 0)
            {
                // if column as original columns then it was introduced by an operator like union
                // as a stand-in for one or more other columns.
                foreach (var oc in column.OriginalColumns)
                {
                    if (globals.GetTable(oc) != null)
                    {
                        if (columnSet.Add(oc))
                            columnList.Add(oc);
                    }
                }
            }
        }

        /// <summary>
        /// Returns a list of all the database table columns in the query result.
        /// </summary>
        public static IReadOnlyList<ColumnSymbol> GetDatabaseTableColumnsInResult(this KustoCode code)
        {
            if (code.ResultType is TableSymbol table)
            {
                var columnSet = new HashSet<ColumnSymbol>();
                var columnList = new List<ColumnSymbol>();

                foreach (var column in table.Columns)
                {
                    AddDatabaseTableColumn(column, columnSet, columnList, code.Globals);
                }

                return columnList;
            }
            else
            {
                return Array.Empty<ColumnSymbol>();
            }
        }

        /// <summary>
        /// Returns a list of all columns referenced or declared in the sub-tree.
        /// </summary>
        private static IReadOnlyList<ColumnSymbol> GetColumnsReferenced(SyntaxNode node)
        {
            var columnSet = new HashSet<ColumnSymbol>();
            var columnList = new List<ColumnSymbol>();

            GatherColumns(node);

            return columnList;

            void GatherColumns(SyntaxNode root)
            {
                SyntaxElement.WalkNodes(root,
                    fnBefore: n =>
                    {
                        if (n.ReferencedSymbol is ColumnSymbol c)
                        {
                            if (columnSet.Add(c))
                                columnList.Add(c);
                        }

                        if (n.GetCalledFunctionBody() is SyntaxNode body)
                        {
                            GatherColumns(body);
                        }
                    },
                    fnDescend: n =>
                        // skip descending into function declarations since their bodies will be examined by the code above
                        !(n is FunctionDeclaration)
                    );
            }
        }

        /// <summary>
        /// Returns all the columns in the result of the query.
        /// </summary>
        public static IReadOnlyList<ColumnSymbol> GetResultColumns(this KustoCode code)
        {
            if (code.ResultType is TableSymbol table)
            {
                return table.Columns;
            }
            else
            {
                return Array.Empty<ColumnSymbol>();
            }
        }

        /// <summary>
        /// Returns the set of database table columns that were used in producing the query result.
        /// </summary>
        public static IReadOnlyList<ColumnSymbol> GetSourceColumns(this KustoCode code, bool includeIncidentalColumns = true)
        {
            return GetSourceColumns(code, GetResultColumns(code), includeIncidentalColumns);
        }

        /// <summary>
        /// Returns the set of database table columns that were used in producing the specified column.
        /// </summary>
        public static IReadOnlyList<ColumnSymbol> GetSourceColumns(this KustoCode code, ColumnSymbol column, bool includeIncidentalColumns = true)
        {
            return GetSourceColumns(code, new[] {column}, includeIncidentalColumns);
        }

        /// <summary>
        /// Returns the set of database table columns that were used in producing the specified columns.
        /// </summary>
        public static IReadOnlyList<ColumnSymbol> GetSourceColumns(this KustoCode code, IReadOnlyList<ColumnSymbol> columns, bool includeIncidentalColumns = true)
        {
            return GetSourceColumns(code.Syntax, code.Globals, columns, includeIncidentalColumns);
        }

        /// <summary>
        /// Returns the set of database table columns that were used in producing the specified columns.
        /// </summary>
        private static IReadOnlyList<ColumnSymbol> GetSourceColumns(
            SyntaxNode root, GlobalState globals, IReadOnlyList<ColumnSymbol> columns, bool includeIncidentalColumns)
        { 
            var columnToOriginMap = GetColumnOrigins(root, globals);

            var columnSet = new HashSet<ColumnSymbol>();
            var columnList = new List<ColumnSymbol>();

            foreach (var c in columns)
            {
                GatherSourceColumns(c);
            }

            if (includeIncidentalColumns)
            {
                // aslo add any explicitly referenced database table column in query, but not explicitly tied
                // to one of the columns (like a column referenced in a where operator)
                var dbColumns = GetDatabaseTableColumnsReferenced(root, globals);
                foreach (var dbCol in dbColumns)
                {
                    AddDatabaseTableColumn(dbCol, columnSet, columnList, globals);
                }
            }

            return columnList;

            void GatherSourceColumns(ColumnSymbol col)
            {
                if (globals.GetTable(col) != null)
                {
                    if (columnSet.Add(col))
                        columnList.Add(col);
                }
                else if (col.OriginalColumns.Count > 0)
                {
                    foreach (var oc in col.OriginalColumns)
                    {
                        GatherSourceColumns(oc);
                    }
                }
                else if (columnToOriginMap.TryGetValue(col, out var origin))
                {
                    var source = GetSource(origin);
                    if (source != null)
                    {
                        var colRefs = GetColumnsReferenced(source);
                        foreach (var cr in colRefs)
                        {
                            GatherSourceColumns(cr);
                        }
                    }
                }
            }

            /// Returns the node the encompasses the source of the declaring node.
            /// example: if the declaring node is the name declaration, the source is the let statement expression.
            static SyntaxNode GetSource(SyntaxNode declaringNode)
            {
                if (declaringNode is Name n)
                    declaringNode = declaringNode.Parent;

                if (declaringNode is NameDeclaration nd)
                {
                    if (nd.Parent is SimpleNamedExpression sne)
                    {
                        return sne.Expression;
                    }
                    else if (nd.Parent is SeparatedElement sep
                        && sep.Parent is SyntaxList<SeparatedElement<NameDeclaration>> nameList
                        && nameList.Parent is CompoundNamedExpression cne)
                    {
                        // TODO: how to associate individual names to arguments in functions?
                        // currently all named tuple parts will be associated with all arguments.
                        // probably will need to handle this per built-in function/aggregate/plugin.
                        return cne.Expression;
                    }
                    else if (nd.Parent is LetStatement ls)
                    {
                        return ls.Expression;
                    }
                    else
                    {
                        return null;
                    }
                }
                else if (declaringNode is NameReference nr
                         && nr.Parent is FunctionCallExpression fc)
                {
                    return fc;
                }
                else
                {
                    return declaringNode;
                }
            }
        }

        /// <summary>
        /// Gets the declaration or first node that introduces a column for all non database columns in the query.
        /// </summary>
        private static IReadOnlyDictionary<ColumnSymbol, SyntaxNode> GetColumnOrigins(
            SyntaxNode root, GlobalState globals)
        {
            var columnToOriginMap = new Dictionary<ColumnSymbol, SyntaxNode>();
            var bodyToCallSiteMap = new Dictionary<SyntaxNode, SyntaxNode>();

            GatherOrigins(root);

            return columnToOriginMap;

            void GatherOrigins(SyntaxNode node)
            {
                SyntaxElement.WalkNodes(node,
                    fnBefore: n =>
                    {
                        if (n is NameDeclaration 
                            && n.ReferencedSymbol is ColumnSymbol c
                            && globals.GetTable(c) == null)
                        {
                            columnToOriginMap[c] = n;
                        }

                        if (n is Expression e)
                        {
                            if (e.ResultType is TableSymbol tb
                                && globals.GetDatabase(tb) == null)
                            {
                                foreach (var col in tb.Columns)
                                {
                                    SetOrigin(col, e);
                                }
                            }
                            else if (e.ResultType is TupleSymbol tp)
                            {
                                foreach (var col in tp.Columns)
                                {
                                    SetOrigin(col, e);
                                }
                            }

                            if (n.GetCalledFunctionBody() is SyntaxNode body)
                            {
                                bodyToCallSiteMap[body] = n;
                                GatherOrigins(body);
                            }
                        }
                    },
                    fnDescend: n =>
                        // skip descending into function declarations since their bodies will be examined by the code above
                        !(n is FunctionDeclaration)
                    );
            }

            void SetOrigin(ColumnSymbol col, SyntaxNode node)
            {
                if (globals.GetTable(col) == null)
                {
                    if (columnToOriginMap.TryGetValue(col, out var currentOrigin))
                    {
                        // actual name declarations are absolutely best origin locations
                        if (currentOrigin is NameDeclaration)
                        {
                            return;
                        }
                        else if (node is NameDeclaration)
                        {
                            columnToOriginMap[col] = node;
                            return;
                        }

                        // try to find comparable location in the same tree.
                        while (!currentOrigin.Root.IsAncestorOf(node)
                            && bodyToCallSiteMap.TryGetValue(node, out var callsite))
                        {
                            node = callsite;
                        }

                        // if both origins are in same tree, then take the earlier node.
                        if (currentOrigin.Root.IsAncestorOf(node))
                        {
                            var earlier = GetBetterOrigin(currentOrigin, node);
                            if (earlier != currentOrigin)
                            {
                                columnToOriginMap[col] = earlier;
                            }
                        }
                    }
                    else
                    {
                        columnToOriginMap[col] = node;
                    }
                }
            }

            static SyntaxNode GetBetterOrigin(SyntaxNode a, SyntaxNode b)
            {
                if (a == null)
                    return b;

                if (b == null)
                    return a;

                // declarations can only occur once
                if (a is NameDeclaration)
                    return a;

                if (b is NameDeclaration)
                    return b;

                // a occurs before b
                if (a.End <= b.TextStart)
                    return a;

                // b occurs before a
                if (b.End <= a.TextStart)
                    return b;

                // b is input to a
                if (a.IsAncestorOf(b))
                    return b;

                // a is input to b
                if (b.IsAncestorOf(a))
                    return a;

                // overlap but no heirarchical relationship?  pick one
                if (a.TextStart <= b.TextStart)
                    return a;

                return b;
            }
        }
    }
}