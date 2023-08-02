using System;
using System.Collections.Generic;
using Kusto.Language;
using Kusto.Language.Symbols;
using Kusto.Language.Syntax;
using Kusto.Language.Utils;

#nullable disable // some day...

namespace Kusto.Toolkit
{
    public static partial class KustoCodeExtensions
    {
        /// <summary>
        /// Returns the set of database table columns that contributed to the data contained in the result columns.
        /// </summary>
        public static IReadOnlyList<ColumnSymbol> GetSourceColumns(this KustoCode code)
        {
            return GetSourceColumns(code, GetResultColumns(code));
        }

        /// <summary>
        /// Returns the set of database table columns that contributed to the data contained in the specified column.
        /// </summary>
        public static IReadOnlyList<ColumnSymbol> GetSourceColumns(this KustoCode code, ColumnSymbol column)
        {
            return GetSourceColumns(code, new[] { column });
        }

        /// <summary>
        /// Returns the set of database table columns that contributed to the data contained in the specified columns.
        /// </summary>
        public static IReadOnlyList<ColumnSymbol> GetSourceColumns(this KustoCode code, IReadOnlyList<ColumnSymbol> columns)
        {
            return GetSourceColumns(columns, code.Globals);
        }

        /// <summary>
        /// Returns the a map between the query's result columns and the set of database table columns that contributed to them.
        /// </summary>
        public static IReadOnlyDictionary<ColumnSymbol, IReadOnlyList<ColumnSymbol>> GetSourceColumnMap(this KustoCode code)
        {
            return GetSourceColumnMap(code, GetResultColumns(code));
        }

        /// <summary>
        /// Returns the a map between specified result columns and the set of database table columns that contributed to them.
        /// </summary>
        private static IReadOnlyDictionary<ColumnSymbol, IReadOnlyList<ColumnSymbol>> GetSourceColumnMap(
            this KustoCode code, IReadOnlyList<ColumnSymbol> columns)
        {
            return GetSourceColumnMap(columns, code.Globals);
        }

        /// <summary>
        /// Returns the a map between the specified columns and the set of database table columns that contributed to them.
        /// </summary>
        private static IReadOnlyList<ColumnSymbol> GetSourceColumns(
            IReadOnlyList<ColumnSymbol> columns, GlobalState globals)
        {
            var columnSet = new HashSet<ColumnSymbol>();
            var columnList = new List<ColumnSymbol>();

            foreach (var c in columns)
            {
                GatherSourceColumnsFromColumn(c, globals, columnSet, columnList);
            }

            return columnList;
        }

        /// <summary>
        /// Returns the a map between specified result columns and the set of database table columns that contributed to them.
        /// </summary>
        private static IReadOnlyDictionary<ColumnSymbol, IReadOnlyList<ColumnSymbol>> GetSourceColumnMap(
            IReadOnlyList<ColumnSymbol> columns, GlobalState globals)
        {
            var columnMap = new Dictionary<ColumnSymbol, IReadOnlyList<ColumnSymbol>>();
            var columnSet = new HashSet<ColumnSymbol>();
            var columnList = new List<ColumnSymbol>();

            foreach (var c in columns)
            {
                columnSet.Clear();
                columnList.Clear();

                GatherSourceColumnsFromColumn(c, globals, columnSet, columnList);

                columnMap.Add(c, columnList.ToReadOnly());
            }

            return columnMap;
        }

        /// <summary>
        /// Gathers a list of database table columns that are the source of the data in the specified column.
        /// </summary>
        private static void GatherSourceColumnsFromColumn(
            ColumnSymbol column,
            GlobalState globals,
            HashSet<ColumnSymbol> columnSet,
            List<ColumnSymbol> columnList)
        {
            if (globals.GetTable(column) != null)
            {
                // This is a database table column. It is what we are looking for.
                if (columnSet.Add(column))
                    columnList.Add(column);
            }
            else if (column.OriginalColumns.Count > 0)
            {
                // Columns that have original-columns were created by semantic analysis
                // to merge multiple columns into one (example: join and union operations).
                foreach (var oc in column.OriginalColumns)
                {
                    GatherSourceColumnsFromColumn(oc, globals, columnSet, columnList);
                }
            }
            else if (column.Source is Expression source)
            {
                // Columns with source expressions were declared and initialized by some expression within the query
                // (examples: named projection expressions, function call arguments)
                GatherSourceColumnsFromSyntax(source, globals, columnSet, columnList);
            }
        }

        /// <summary>
        /// Gathers a list of database table columns that are either referenced in the syntax
        /// or are the source columns of the column or variable referenced in the syntax.
        /// </summary>
        private static void GatherSourceColumnsFromSyntax(SyntaxNode node, GlobalState globals, HashSet<ColumnSymbol> columnSet, List<ColumnSymbol> columnList)
        {
            SyntaxElement.WalkNodes(node,
                fnBefore: n =>
                {
                    if (n is Expression e)
                    {
                        if (e.ReferencedSymbol is ColumnSymbol column)
                        {
                            GatherSourceColumnsFromColumn(column, globals, columnSet, columnList);
                        }
                        else if (e.ReferencedSymbol is VariableSymbol variable)
                        {
                            GatherSourceColumnsFromVariable(variable, globals, columnSet, columnList);
                        }
                        else if (e.GetCalledFunctionBody() is SyntaxNode body)
                        {
                            GatherSourceColumnsFromSyntax(body, globals, columnSet, columnList);
                        }
                    }
                },
                fnAfter: n =>
                {
                    if (n.Alternates != null)
                    {
                        foreach (var alt in n.Alternates)
                        {
                            GatherSourceColumnsFromSyntax(alt, globals, columnSet, columnList);
                        }
                    }
                },
                // don't look inside function declarations, we already do this when we recurse into called function bodies.
                fnDescend: n => !(n is FunctionDeclaration)
                );
        }

        /// <summary>
        /// Gathers the database table columns that are the source of the data in the let-variable.
        /// </summary>
        private static void GatherSourceColumnsFromVariable(VariableSymbol variable, GlobalState globals, HashSet<ColumnSymbol> columnSet, List<ColumnSymbol> columnList)
        {
            if (variable.Source is Expression varSource)
            {
                GatherSourceColumnsFromSyntax(varSource, globals, columnSet, columnList);
            }
        }
    }
}
