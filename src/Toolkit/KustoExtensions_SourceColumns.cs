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
        /// Returns the set of database table columns that contributed to the data contained in the specified columns.
        /// </summary>
        public static IReadOnlyList<ColumnSymbol> GetSourceColumns(this KustoCode code)
        {
            return GetSourceColumns(code, GetResultColumns(code));
        }

        /// <summary>
        /// Returns the set of database table columns that contributed to the data contained in the specified columns.
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
            return GetSourceColumns(code.Syntax, code.Globals, columns);
        }

        /// <summary>
        /// Returns the set of database table columns that contributed to the data contained in the specified columns.
        /// </summary>
        private static IReadOnlyList<ColumnSymbol> GetSourceColumns(
            SyntaxNode root, GlobalState globals, IReadOnlyList<ColumnSymbol> columns)
        {
            var columnToOriginMap = GetSymbolOrigins(root, globals);

            var columnSet = new HashSet<ColumnSymbol>();
            var columnList = new List<ColumnSymbol>();

            foreach (var c in columns)
            {
                GatherSourceColumns(c);
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
                    var source = GetSourceFromOrigin(origin, col);
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
        }

        /// <summary>
        /// Returns the source expression that computes the symbol first introduced at the origin.
        /// examples:
        ///    if the origin is the name of a let variable declaration, the source is the let statement's expression.
        ///    if the origin is a projection operator, the source is the projection expression that computes it.
        ///    if the origin is a function call, the source may be the entire function call or one of its arguments.
        /// </summary>
        private static Expression GetSourceFromOrigin(SyntaxNode origin, Symbol symbol)
        {
            if (origin is Name n)
                origin = origin.Parent;

            if (origin is NameDeclaration nd)
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
            else if (origin is NameReference nr
                     && nr.Parent is FunctionCallExpression fc)
            {
                return fc;
            }
            else if (origin is QueryOperator qo
                && symbol is ColumnSymbol col)
            {
                // if node that first introduces column is a query operator, try to get a better
                // source expression than the entire operator
                return GetProjectionExpressionForColumn(qo, col) ?? qo;
            }
            else
            {
                return origin as Expression;
            }
        }

        /// <summary>
        /// Gets the projection expression for the specified column within the query operator.
        /// </summary>
        static Expression GetProjectionExpressionForColumn(QueryOperator op, ColumnSymbol col)
        {
            if (op.ResultType is TableSymbol ts)
            {
                var columnIndex = ts.Columns.IndexOf(col);
                if (columnIndex >= 0)
                {
                    switch (op)
                    {
                        case ProjectOperator pop:
                            return GetProjectionExpressionForIndex(pop.Expressions, columnIndex);

                        case ExtendOperator exop:
                            return GetProjectionExpressionForIndex(exop.Expressions, columnIndex);

                        case DistinctOperator dop:
                            return GetProjectionExpressionForIndex(dop.Expressions, columnIndex);

                        case SerializeOperator sop:
                            return GetProjectionExpressionForIndex(sop.Expressions, columnIndex);

                        case SummarizeOperator sumo:
                            if (sumo.ByClause != null)
                            {
                                return GetProjectionExpressionForIndex(sumo.ByClause.Expressions, columnIndex)
                                    ?? GetProjectionExpressionForIndex(sumo.Aggregates, columnIndex - GetProjectionColumnCount(sumo.ByClause.Expressions));
                            }
                            else
                            {
                                return GetProjectionExpressionForIndex(sumo.Aggregates, columnIndex);
                            }

                        case GraphMatchOperator gmop:
                            if (gmop.ProjectClause != null)
                            {
                                return GetProjectionExpressionForIndex(gmop.ProjectClause.Expressions, columnIndex);
                            }
                            break;
                    }
                }
            }

            return null;
        }

        /// <summary>
        /// Gets the expression associated with the nth projected column.
        /// </summary>
        static Expression GetProjectionExpressionForIndex(SyntaxList<SeparatedElement<Expression>> projectionList, int columnIndex)
        {
            int offset = columnIndex;

            for (int iElement = 0; iElement < projectionList.Count; iElement++)
            {
                var element = projectionList[iElement].Element;

                if (offset == 0)
                {
                    return element;
                }
                else if (element.RawResultType is TupleSymbol tu)
                {
                    if (offset < tu.Columns.Count)
                    {
                        return element;
                    }
                    else
                    {
                        offset -= tu.Columns.Count;
                    }
                }
                else
                {
                    offset -= 1;
                }
            }

            // columnIndex not in project list
            return null;
        }

        /// <summary>
        /// Gets the number of column produced by the projection list
        /// </summary>
        private static int GetProjectionColumnCount(SyntaxList<SeparatedElement<Expression>> projectionList)
        {
            int columnCount = 0;

            for (int iElement = 0; iElement < projectionList.Count; iElement++)
            {
                var element = projectionList[iElement].Element;

                if (element.RawResultType is TupleSymbol tu)
                {
                    columnCount += tu.Columns.Count;
                }
                else
                {
                    columnCount++;
                }
            }

            return columnCount;
        }

        /// <summary>
        /// Returns a map between symbols and the origin node for each symbol declared or introduced by the syntax.
        /// </summary>
        private static IReadOnlyDictionary<Symbol, SyntaxNode> GetSymbolOrigins(
            SyntaxNode root, GlobalState globals)
        {
            var symbolToOriginMap = new Dictionary<Symbol, SyntaxNode>();
            var bodyToCallSiteMap = new Dictionary<SyntaxNode, SyntaxNode>();

            GatherOrigins(root);

            return symbolToOriginMap;

            void GatherOrigins(SyntaxNode node)
            {
                SyntaxElement.WalkNodes(node,
                    fnBefore: n =>
                    {
                        if (n is NameDeclaration
                            && n.ReferencedSymbol is ColumnSymbol c
                            && globals.GetTable(c) == null)
                        {
                            symbolToOriginMap[c] = n;
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

            void SetOrigin(Symbol symbol, SyntaxNode node)
            {
                // known database table columns do not have origins in source
                if (symbol is ColumnSymbol col && globals.GetTable(col) != null)
                    return;

                if (symbolToOriginMap.TryGetValue(symbol, out var currentOrigin))
                {
                    // actual name declarations are absolutely best origin locations
                    if (currentOrigin is NameDeclaration)
                    {
                        return;
                    }
                    else if (node is NameDeclaration)
                    {
                        symbolToOriginMap[symbol] = node;
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
                            symbolToOriginMap[symbol] = earlier;
                        }
                    }
                }
                else
                {
                    symbolToOriginMap[symbol] = node;
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
