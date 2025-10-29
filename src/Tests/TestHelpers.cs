using Kusto.Language.Symbols;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Tests
{
    public static class TestHelpers
    {
        /// <summary>
        /// Converts the symbol to string for test comparison purposes.
        /// </summary>
        public static string ToTestString(this Symbol symbol)
        {
            switch (symbol)
            {
                case TableSymbol ts:
                    return $"({string.Join(", ", ts.Columns.Select(c => c.ToTestString()))})";
                case ColumnSymbol cs:
                    return $"{cs.Name}: {cs.Type.ToTestString()}";
                case ScalarSymbol ss:
                    return ss.Name;
                case null:
                    return "";
                default:
                    throw new InvalidOperationException($"Unhandled symbol kind: {symbol.GetType().Name}");
            }
        }
    }

}
