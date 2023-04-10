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
        /// Returns a list of all the database tables that are referenced in the query.
        /// </summary>
        public static IReadOnlyList<TableSymbol> GetDatabaseTablesReferenced(this KustoCode code)
        {
            return code.GetDatabaseMembersReferenced<TableSymbol>();
        }

        /// <summary>
        /// Returns a list of all the stored functions that are used in the query.
        /// </summary>
        public static IReadOnlyList<FunctionSymbol> GetStoredFunctionsReferenced(this KustoCode code)
        {
            return code.GetDatabaseMembersReferenced<FunctionSymbol>();
        }

        /// <summary>
        /// Returns a list of all the external tables referenced in the query.
        /// </summary>
        public static IReadOnlyList<ExternalTableSymbol> GetExternalTablesReferenced(this KustoCode code)
        {
            return code.GetDatabaseMembersReferenced<ExternalTableSymbol>();
        }

        /// <summary>
        /// Returns a list of all the materialized views referenced in the query.
        /// </summary>
        public static IReadOnlyList<MaterializedViewSymbol> GetMaterializedViewsReferenced(this KustoCode code)
        {
            return code.GetDatabaseMembersReferenced<MaterializedViewSymbol>();
        }

        /// <summary>
        /// Returns a list of all the database members (tables, functions, external tables, materialized views, etc) that are used in the query.
        /// </summary>
        public static IReadOnlyList<TMember> GetDatabaseMembersReferenced<TMember>(this KustoCode code, Func<TMember, bool>? predicate = null)
            where TMember : Symbol
        {
            var memberSet = new HashSet<TMember>();
            var memberList = new List<TMember>();
            GatherMembers(code.Syntax);
            return memberList;

            void GatherMembers(SyntaxNode root)
            {
                SyntaxElement.WalkNodes(root,
                    fnBefore: n =>
                    {
                        if (n.ReferencedSymbol is GroupSymbol referencedGroup)
                        {
                            AddGroupMembers(referencedGroup);
                        }
                        else if (n.ReferencedSymbol is TMember member)
                        {
                            AddMember(member);
                        }

                        if (n is Expression e)
                        {
                            if (e.ResultType is GroupSymbol resultGroup)
                            {
                                AddGroupMembers(resultGroup);
                            }
                            else if (e.ResultType is TMember member)
                            {
                                AddMember(member);
                            }
                        }

                        if (n.GetCalledFunctionBody() is SyntaxNode body)
                        {
                            GatherMembers(body);
                        }
                    },
                    fnDescend: n =>
                        // skip descending into function declarations since their bodies will be examined by the code above
                        !(n is FunctionDeclaration)
                    );
            }

            bool Matches(TMember member)
            {
                // if TMember is TableSymbol, only match exact type, not sub types
                if (typeof(TMember) == typeof(TableSymbol)
                    && member.GetType() != typeof(TMember))
                {
                    return false;
                }

                return predicate == null || predicate(member);
            }

            void AddMember(TMember member)
            {
                if (code.Globals.IsDatabaseSymbol(member) 
                    && Matches(member))
                {
                    if (memberSet.Add(member))
                        memberList.Add(member);
                }
            }

            void AddGroupMembers(GroupSymbol group)
            {
                if (group.Members.Count > 0)
                {
                    foreach (var groupMember in group.Members)
                    {
                        if (groupMember is TMember member)
                        {
                            AddMember(member);
                        }
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