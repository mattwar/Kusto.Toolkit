using System;
using System.Collections.Generic;
using Kusto.Language;
using Kusto.Language.Symbols;
using Kusto.Language.Syntax;
using Kusto.Language.Utils;

namespace Kusto.Toolkit
{
    public static partial class KustoCodeExtensions
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
                        if (n.ReferencedSymbol is TableSymbol ntab)
                        {
                            AddTable(ntab);
                        }
                        else if (n.ReferencedSymbol is GroupSymbol ng)
                        {
                            AddGroupTables(ng);
                        }
    
                        if (n is Expression e)
                        {
                            if (e.ResultType is TableSymbol ts)
                            {
                                AddTable(ts);
                            }
                            else if (e.ResultType is GroupSymbol eg)
                            {
                                AddGroupTables(eg);
                            }
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

            void AddTable(TableSymbol table)
            {
                if (code.Globals.IsDatabaseTable(table))
                {
                    if (tableSet.Add(table))
                        tableList.Add(table);
                }
            }

            void AddGroupTables(GroupSymbol group)
            {
                if (group.Members.Count > 0
                    && group.Members[0] is TableSymbol)
                {
                    foreach (var member in group.Members)
                    {
                        if (member is TableSymbol table)
                            AddTable(table);
                    }
                }
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