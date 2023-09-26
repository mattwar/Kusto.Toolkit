using System;
using System.Collections.Generic;
using System.Xml.Linq;
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
            var processedColumns = s_columnSetPool.AllocateFromPool();
            var processedExprs = s_nodeSetPool.AllocateFromPool();
            var columnQueue = s_columnQueuePool.AllocateFromPool();
            var exprQueue = s_nodeQueuePool.AllocateFromPool();

            try
            {
                columnQueue.Enqueue(column);

                while (columnQueue.Count > 0)
                {
                    var c = columnQueue.Dequeue();

                    if (!processedColumns.Add(c))
                        continue;

                    if (globals.GetTable(c) != null)
                    {
                        // This is a database table column. It is what we are looking for.
                        if (columnSet.Add(c))
                            columnList.Add(c);
                    }
                    else if (c.OriginalColumns.Count > 0)
                    {
                        // Columns that have original-columns were created by semantic analysis
                        // to merge multiple columns into one (example: join and union operations).
                        foreach (var oc in c.OriginalColumns)
                        {
                            columnQueue.Enqueue(oc);
                        }
                    }
                    else if (c.Source is Expression source)
                    {
                        // Columns with source expressions were declared and initialized by some expression within the query
                        // (examples: named projection expressions, function call arguments)
                        exprQueue.Enqueue(source);

                        while (exprQueue.Count > 0)
                        {
                            var node = exprQueue.Dequeue();

                            if (!processedExprs.Add(node))
                                continue;

                            SyntaxElement.WalkNodes(node,
                                fnBefore: n =>
                                {
                                    if (n is Expression e)
                                    {
                                        if (e.ReferencedSymbol is ColumnSymbol refColumn)
                                        {
                                            columnQueue.Enqueue(refColumn);
                                        }
                                        else if (e.ReferencedSymbol is VariableSymbol variable
                                            && variable.Source is Expression varSource)
                                        {
                                            exprQueue.Enqueue(varSource);
                                        }
                                        else if (e.GetCalledFunctionBody() is SyntaxNode body)
                                        {
                                            exprQueue.Enqueue(body);
                                        }
                                    }
                                },
                                fnAfter: n =>
                                {
                                    if (n.Alternates != null)
                                    {
                                        foreach (var alt in n.Alternates)
                                        {
                                            exprQueue.Enqueue(alt);
                                        }
                                    }
                                },
                                // don't look inside function declarations, we already do this when we recurse into called function bodies.
                                fnDescend: n => !(n is FunctionDeclaration)
                                );
                        }
                    }
                }
            }
            finally
            {
                s_columnSetPool.ReturnToPool(processedColumns);
                s_nodeSetPool.ReturnToPool(processedExprs);
                s_columnQueuePool.ReturnToPool(columnQueue);
                s_nodeQueuePool.ReturnToPool(exprQueue);
            }
        }
    }
}
