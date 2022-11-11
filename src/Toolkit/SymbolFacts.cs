using Kusto.Language;
using Kusto.Language.Symbols;

#nullable disable // some day...

namespace Kusto.Toolkit
{
    /// <summary>
    /// Helpful utility methods for operating with symbols.
    /// </summary>
    public static class SymbolFacts
    {
        public static string GetFullHostName(string clusterNameOrUri, string defaultDomain)
        {
            return KustoFacts.GetFullHostName(KustoFacts.GetHostName(clusterNameOrUri), defaultDomain);
        }

        /// <summary>
        /// Convert CLR type name into a Kusto scalar type.
        /// </summary>
        public static ScalarSymbol GetKustoType(string clrTypeName)
        {
            switch (clrTypeName)
            {
                case "System.Byte":
                case "Byte":
                case "byte":
                case "System.SByte":
                case "SByte":
                case "sbyte":
                case "System.Int16":
                case "Int16":
                case "short":
                case "System.UInt16":
                case "UInt16":
                case "ushort":
                case "System.Int32":
                case "Int32":
                case "int":
                    return ScalarTypes.Int;
                case "System.UInt32": // unsigned ints don't fit into int, use long
                case "UInt32":
                case "uint":
                case "System.Int64":
                case "Int64":
                case "long":
                    return ScalarTypes.Long;
                case "System.Double":
                case "Double":
                case "double":
                case "float":
                case "System.single":
                case "System.Single":
                    return ScalarTypes.Real;
                case "System.UInt64": // unsigned longs do not fit into long, use decimal
                case "UInt64":
                case "ulong":
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

        /// <summary>
        /// Gets the schema representation of a table as it would be represented in Kusto.
        /// </summary>
        public static string GetSchema(TableSymbol table)
        {
            return "(" + string.Join(", ", table.Columns.Select(c => $"{KustoFacts.BracketNameIfNecessary(c.Name)}: {GetKustoType(c.Type)}")) + ")";
        }

        /// <summary>
        /// Gets the text for a type/schema declaration as it would be represented in a kusto query.
        /// </summary>
        public static string GetKustoType(TypeSymbol type)
        {
            if (type is ScalarSymbol s)
            {
                return s.Name;
            }
            else if (type is TableSymbol t)
            {
                if (t.Columns.Count == 0)
                {
                    return "(*)";
                }
                else
                {
                    return GetSchema(t);
                }
            }
            else
            {
                return "unknown";
            }
        }

        /// <summary>
        /// Gets the text for a parameter type/schema declaration as it would be represented a kusto query.
        /// /// </summary>
        public static string GetFunctionParameterType(Parameter p)
        {
            switch (p.TypeKind)
            {
                case ParameterTypeKind.Declared:
                    return GetKustoType(p.DeclaredTypes[0]);
                case ParameterTypeKind.Tabular:
                    return "(*)";
                default:
                    return "unknown";
            }
        }

        /// <summary>
        /// Gets the text of a parameter list of the specified function as it would appear in a kusto query.
        /// </summary>
        public static string GetParameterList(FunctionSymbol fun)
        {
            return GetParameterList(fun.Signatures[0]);
        }

        /// <summary>
        /// Gets the text of the parameter list for the specified function signature as it would appear in a kusto query.
        /// </summary>
        private static string GetParameterList(Signature sig)
        {
            return "(" + string.Join(", ", sig.Parameters.Select(p => $"{KustoFacts.BracketNameIfNecessary(p.Name)}: {GetFunctionParameterType(p)}")) + ")";
        }
    }
}
