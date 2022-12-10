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
            return GetSourceColumns(code.Syntax, code.Globals, columns);
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
            return GetSourceColumnMap(code.Syntax, code.Globals, columns);
        }

        /// <summary>
        /// Returns the set of database table columns that contributed to the data contained in the specified columns.
        /// </summary>
        private static IReadOnlyList<ColumnSymbol> GetSourceColumns(
            SyntaxNode root, GlobalState globals, IReadOnlyList<ColumnSymbol> columns)
        {
            var symbolToSourceMap = GetSymbolSources(root, globals);

            var columnSet = new HashSet<ColumnSymbol>();
            var columnList = new List<ColumnSymbol>();

            foreach (var c in columns)
            {
                GatherSourceColumns(c, globals, symbolToSourceMap, columnSet, columnList);
            }

            return columnList;
        }

        /// <summary>
        /// Returns the a map between specified result columns and the set of database table columns that contributed to them.
        /// </summary>
        private static IReadOnlyDictionary<ColumnSymbol, IReadOnlyList<ColumnSymbol>> GetSourceColumnMap(
            SyntaxNode root, GlobalState globals, IReadOnlyList<ColumnSymbol> columns)
        {
            var symbolToSourceMap = GetSymbolSources(root, globals);

            var columnMap = new Dictionary<ColumnSymbol, IReadOnlyList<ColumnSymbol>>();
            var columnSet = new HashSet<ColumnSymbol>();
            var columnList = new List<ColumnSymbol>();

            foreach (var c in columns)
            {
                columnSet.Clear();
                columnList.Clear();

                GatherSourceColumns(c, globals, symbolToSourceMap, columnSet, columnList);

                columnMap.Add(c, columnList.ToReadOnly());
            }

            return columnMap;
        }

        /// <summary>
        /// Returns the set of database table columns that contributed to the data contained in the specified columns.
        /// </summary>
        private static void GatherSourceColumns(
            ColumnSymbol column,
            GlobalState globals,
            IReadOnlyDictionary<Symbol, Expression> symbolToSourceMap,
            HashSet<ColumnSymbol> columnSet,
            List<ColumnSymbol> columnList)
        {
            if (globals.GetTable(column) != null)
            {
                if (columnSet.Add(column))
                    columnList.Add(column);
            }
            else if (column.OriginalColumns.Count > 0)
            {
                foreach (var oc in column.OriginalColumns)
                {
                    GatherSourceColumns(oc, globals, symbolToSourceMap, columnSet, columnList);
                }
            }
            else if (symbolToSourceMap.TryGetValue(column, out var source))
            {
                var inputColumns = GetSourceColumns(source, globals, symbolToSourceMap);
                foreach (var ic in inputColumns)
                {
                    if (columnSet.Add(ic))
                        columnList.Add(ic);
                }
            }
        }

        /// <summary>
        /// Returns the list of database table columns that the contributed to the expression result.
        /// </summary>
        private static IReadOnlyList<ColumnSymbol> GetSourceColumns(Expression expr, GlobalState globals, IReadOnlyDictionary<Symbol, Expression> symbolToSourceMap)
        {
            var columnSet = new HashSet<ColumnSymbol>();
            var columnList = new List<ColumnSymbol>();

            GatherSourceColumns(expr);

            return columnList;

            void GatherSourceColumns(SyntaxNode node)
            {
                SyntaxElement.WalkNodes(node,
                    fnBefore: n =>
                    {
                        if (n is Expression e)
                        {
                            if (e.ReferencedSymbol is ColumnSymbol column)
                            {
                                if (globals.GetTable(column) != null)
                                {
                                    if (columnSet.Add(column))
                                        columnList.Add(column);
                                    return;
                                }
                                else if (column.OriginalColumns.Count > 0)
                                {
                                    foreach (var oc in column.OriginalColumns)
                                    {
                                        if (globals.GetTable(oc) != null)
                                        {
                                            if (columnSet.Add(oc))
                                                columnList.Add(oc);
                                        }
                                        else if (symbolToSourceMap.TryGetValue(oc, out var ocSource))
                                        {
                                            GatherSourceColumns(ocSource);
                                        }
                                    }
                                }
                                else if (symbolToSourceMap.TryGetValue(column, out var colSource))
                                {
                                    GatherSourceColumns(colSource);
                                }
                            }
                            else if (e.ReferencedSymbol is VariableSymbol variable)
                            {
                                if (symbolToSourceMap.TryGetValue(variable, out var varSource))
                                {
                                    GatherSourceColumns(varSource);
                                }
                            }
                            else if (e.GetCalledFunctionBody() is SyntaxNode body)
                            {
                                GatherSourceColumns(body);
                            }
                        }
                    },
                    fnDescend: n => !(n is FunctionDeclaration)
                    );
            }
        }

        /// <summary>
        /// Returns a map between the symbols declared in the syntax and their source expressions.
        /// </summary>
        private static IReadOnlyDictionary<Symbol, Expression> GetSymbolSources(SyntaxNode root, GlobalState globals)
        {
            var sources = new Dictionary<Symbol, Expression>();
            var origins = GetSymbolOrigins(root, globals);

            foreach (var pair in origins)
            {
                var symbol = pair.Key;
                var origin = pair.Value;
                var source = GetSourceFromOrigin(origin, symbol);
                if (source != null)
                {
                    sources.Add(symbol, source);
                }
            }

            return sources;
        }

        /// <summary>
        /// Returns the source expression that computes the symbol first introduced at the origin.
        /// examples:
        ///    if the origin is the name of a let variable declaration, the source is the let statement's expression.
        ///    if the origin is a projection-like operator, the source is the projection expression that computes it.
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
        /// Returns true if the symbol is an external entity defined within the global state.
        /// </summary>
        private static bool IsExternalEntity(GlobalState globals, Symbol symbol)
        {
            switch (symbol)
            {
                case ColumnSymbol col:
                    return globals.GetTable(col) != null;
                case TableSymbol _:
                case FunctionSymbol _:
                    return globals.GetDatabase(symbol) != null;
                case DatabaseSymbol _:
                case ClusterSymbol _:
                    return true;
                default:
                    return false;
            }
        }

        /// <summary>
        /// Returns a map between symbols and the origin node for each symbol declared or introduced by the syntax tree.
        /// </summary>
        private static IReadOnlyDictionary<Symbol, SyntaxNode> GetSymbolOrigins(
            SyntaxNode root, GlobalState globals)
        {
            var symbolToOriginMap = new Dictionary<Symbol, SyntaxNode>();
            FunctionCallExpression callsite = null;

            GatherOrigins(root);
            return symbolToOriginMap;

            void GatherOrigins(SyntaxNode node)
            {
                SyntaxElement.WalkNodes(node,
                    fnBefore: n =>
                    {
                        if (n is Expression e)
                        {
                            // any symbol defined by a name declaration
                            if (e is NameDeclaration nd
                                && nd.ReferencedSymbol is Symbol symbol
                                && !IsExternalEntity(globals, symbol))
                            {
                                SetOrigin(symbol, nd);
                            }

                            if (n.GetCalledFunctionBody() is SyntaxNode body)
                            {
                                var old_callsite = callsite;
                                callsite = n as FunctionCallExpression;
                                GatherOrigins(body);
                                callsite = old_callsite;
                            }

                            // tabular and tuple results may be introducing new columns
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
                            
                            // variables not directly declared may be referring to function parameters
                            if (e.ReferencedSymbol is VariableSymbol vs
                                && !symbolToOriginMap.ContainsKey(vs)
                                && callsite != null
                                && callsite.ReferencedSignature is Signature sig
                                && sig.GetParameter(vs.Name) is Parameter prm)
                            {
                                var index = sig.Parameters.IndexOf(prm);
                                if (index >= 0 && index < sig.Parameters.Count)
                                {
                                    var arg = callsite.ArgumentList.Expressions[index].Element;
                                    SetOrigin(vs, arg);
                                }
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
                // external symbols do not have origins in source
                if (IsExternalEntity(globals, symbol))
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

                    // if both origins are in same tree, then take the better origin.
                    if (currentOrigin.Root.IsAncestorOf(node))
                    {
                        var better = GetBetterOrigin(currentOrigin, node);
                        if (better != currentOrigin)
                        {
                            symbolToOriginMap[symbol] = better;
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

                // a occurs entirely before b
                if (a.End <= b.TextStart)
                    return a;

                // b occurs entirely before a
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
