using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Kusto.Language;
using Kusto.Toolkit;
using Kusto.Language.Symbols;

namespace Tests
{
    [TestClass]
    public class SymbolLoaderTestBase
    {
        protected static readonly string HelpConnection = "https://help.kusto.windows.net;Fed=true";
        protected static readonly string TestSchemaPath = "Schema";
        protected static readonly string HelpCluster = "help.kusto.windows.net";

        protected static readonly IReadOnlyList<ClusterSymbol> OneTestCluster = new ClusterSymbol[]
        {
            new ClusterSymbol("cluster1.kusto.windows.net",
                new DatabaseSymbol("db1",
                    new TableSymbol("Table1", "(x: long)")),
                new DatabaseSymbol("db2",
                    new TableSymbol("Table2", "(x: long)")))
        };

        protected static readonly IReadOnlyList<ClusterSymbol> TwoTestClusters = new ClusterSymbol[]
        {
            new ClusterSymbol("cluster1.kusto.windows.net",
                new DatabaseSymbol("db1",
                    new TableSymbol("Table1", "(x: long)")),
                new DatabaseSymbol("db2",
                    new TableSymbol("Table2", "(x: long)"))),

            new ClusterSymbol("cluster2.kusto.windows.net",
                new DatabaseSymbol("db1",
                    new TableSymbol("Table1", "(x: long)")),
                new DatabaseSymbol("db2",
                    new TableSymbol("Table2", "(x: long)")))
        };

        protected static string GetTestCachePath()
        {
            return Path.Combine(Environment.CurrentDirectory, "TestCache_" + System.Guid.NewGuid());
        }

        protected static void AssertEqual(Symbol expected, Symbol actual)
        {
            Assert.AreEqual(expected.GetType(), actual.GetType(), "symbol types");

            switch (expected)
            {
                case ClusterSymbol cs:
                    AssertEqual(cs, (ClusterSymbol)actual);
                    break;
                case DatabaseSymbol db:
                    AssertEqual(db, (DatabaseSymbol)actual);
                    break;
                case MaterializedViewSymbol mview:
                    AssertEqual(mview, (MaterializedViewSymbol)actual);
                    break;
                case ExternalTableSymbol extab:
                    AssertEqual(extab, (ExternalTableSymbol)actual);
                    break;
                case TableSymbol tab:
                    AssertEqual(tab, (TableSymbol)actual);
                    break;
                case FunctionSymbol fun:
                    AssertEqual(fun, (FunctionSymbol)actual);
                    break;
                case EntityGroupSymbol eg:
                    AssertEqual(eg, (EntityGroupSymbol)actual);
                    break;
                default:
                    Assert.Fail($"Unhandled symbol type: {expected.Kind}");
                    break;
            }
        }

        protected static void AssertEqual(ClusterSymbol expected, ClusterSymbol actual)
        {
            Assert.AreEqual(expected.Name, actual.Name, "cluster name");
            Assert.AreEqual(expected.Members.Count, actual.Members.Count, "cluster members count");

            for (int i = 0; i < expected.Members.Count; i++)
            {
                AssertEqual(expected.Members[i], actual.Members[i]);
            }
        }

        protected static void AssertEqual(DatabaseSymbol expected, DatabaseSymbol actual)
        {
            Assert.AreEqual(expected.Name, actual.Name, "database name");
            Assert.AreEqual(expected.AlternateName, actual.AlternateName, "database alternate name");

            Assert.AreEqual(expected.Tables.Count, actual.Tables.Count, "tables count");
            for (int i = 0; i < expected.Tables.Count; i++)
            {
                AssertEqual(expected.Tables[i], actual.Tables[i]);
            }

            Assert.AreEqual(expected.ExternalTables.Count, actual.ExternalTables.Count, "external tables count");
            for (int i = 0; i < expected.ExternalTables.Count; i++)
            {
                AssertEqual(expected.ExternalTables[i], actual.ExternalTables[i]);
            }

            Assert.AreEqual(expected.MaterializedViews.Count, actual.MaterializedViews.Count, "materialized views count");
            for (int i = 0; i < expected.MaterializedViews.Count; i++)
            {
                AssertEqual(expected.MaterializedViews[i], actual.MaterializedViews[i]);
            }

            Assert.AreEqual(expected.EntityGroups.Count, actual.EntityGroups.Count, "entity groups count");
            for (int i = 0; i < expected.EntityGroups.Count; i++)
            {
                AssertEqual(expected.EntityGroups[i], actual.EntityGroups[i]);
            }

            Assert.AreEqual(expected.Functions.Count, actual.Functions.Count, "functions count");
            for (int i = 0; i < expected.Functions.Count; i++)
            {
                AssertEqual(expected.Functions[i], actual.Functions[i]);
            }
        }

        protected static void AssertEqual(TableSymbol expected, TableSymbol actual)
        {
            Assert.AreEqual(expected.Name, actual.Name, "table name");
            Assert.AreEqual(expected.Description, actual.Description, "table description");

            var expectedSchema = SymbolFacts.GetSchema(expected);
            var actualSchema = SymbolFacts.GetSchema(actual);
            Assert.AreEqual(expectedSchema, actualSchema, $"table '{expected.Name}' schema");
        }

        protected static void AssertEqual(ExternalTableSymbol expected, ExternalTableSymbol actual)
        {
            Assert.AreEqual(expected.Name, actual.Name, "external table name");
            Assert.AreEqual(expected.Description, actual.Description, "external table description");

            var expectedSchema = SymbolFacts.GetSchema(expected);
            var actualSchema = SymbolFacts.GetSchema(actual);
            Assert.AreEqual(expectedSchema, actualSchema, $"external table '{expected.Name}' schema");
        }

        protected static void AssertEqual(MaterializedViewSymbol expected, MaterializedViewSymbol actual)
        {
            Assert.AreEqual(expected.Name, actual.Name, "materialized view name");
            Assert.AreEqual(expected.Description, actual.Description, "materialized view description");

            var expectedSchema = SymbolFacts.GetSchema(expected);
            var actualSchema = SymbolFacts.GetSchema(actual);
            Assert.AreEqual(expectedSchema, actualSchema, $"materialized view '{expected.Name}' schema");

            Assert.AreEqual(expected.MaterializedViewQuery, actual.MaterializedViewQuery, $"materialzed view '{expected.Name}' query");
        }

        protected static void AssertEqual(EntityGroupSymbol expected, EntityGroupSymbol actual)
        {
            Assert.AreEqual(expected.Name, actual.Name, "entity group name");
            Assert.AreEqual(expected.Description, actual.Description, "entity group description");
            Assert.AreEqual(expected.Definition, actual.Definition, "entity group definition");
        }

        protected static void AssertEqual(FunctionSymbol expected, FunctionSymbol actual)
        {
            Assert.AreEqual(expected.Name, actual.Name, "function name");
            Assert.AreEqual(expected.Description, actual.Description, "function description");

            var expectedParameters = SymbolFacts.GetParameterList(expected);
            var actualParameters = SymbolFacts.GetParameterList(actual);
            Assert.AreEqual(expectedParameters, actualParameters, $"function '{expected.Name}' parameters");

            var expectedBody = expected.Signatures[0].Body;
            var actualBody = actual.Signatures[0].Body;

            Assert.AreEqual(expectedBody, actualBody, $"function '{expected.Name}' body");
        }
    }
}
