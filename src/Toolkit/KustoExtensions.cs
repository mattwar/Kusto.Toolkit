using System;
using System.Collections.Generic;
using Kusto.Language;
using Kusto.Language.Symbols;
using Kusto.Language.Syntax;
using Kusto.Language.Utils;

#nullable disable // some day...

namespace Kusto.Toolkit
{
    public static partial class KustoExtensions
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
        public static IReadOnlyList<ColumnSymbol> GetColumnsReferenced(SyntaxNode node)
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
    }
}