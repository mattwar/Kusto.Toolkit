using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Kusto.Language;
using Kusto.Language.Syntax;
using Kusto.Language.Editor;
using Kusto.Language.Symbols;
using Kusto.Toolkit;

namespace Tests
{
    [TestClass]
    public class SymbolResolverTests
    {
        [TestMethod]
        public async Task TestAddReferencedDatabasesAsync_ReferenceInQuery()
        {
            var clusters = new ClusterSymbol[]
            {
                new ClusterSymbol("cluster1.kusto.windows.net",
                    new DatabaseSymbol("db1",
                        new TableSymbol("Table1", "(x: long)")),
                    new DatabaseSymbol("db2",
                        new TableSymbol("Table2", "(x: long)")))
            };

            var loader = new TestLoader(clusters, "cluster1");
            var resolver = new SymbolResolver(loader);

            // set default database.
            var globals = await loader.AddOrUpdateDefaultDatabaseAsync(GlobalState.Default, "db1");

            // just one database should exist
            Assert.AreEqual(1, globals.Cluster.Databases.Count);

            // parse query that has explicit reference to other database
            var code = KustoCode.ParseAndAnalyze("database('db2').Table2", globals);

            // use loader to add symbols for any explicity referenced databases
            var newCode = await resolver.AddReferencedDatabasesAsync(code);

            // both databases should exist now
            Assert.AreEqual(2, newCode.Globals.Cluster.Databases.Count);

            // find table in database
            var db2 = newCode.Globals.Cluster.GetDatabase("db2");
            var table2 = db2.GetTable("Table2");

            // verify reference to Table2 actually is Table2
            var expr = newCode.Syntax.GetFirstDescendant<NameReference>(nr => nr.SimpleName == "Table2");
            Assert.AreSame(table2, expr.ResultType);
        }

        [TestMethod]
        public async Task TestAddReferencedDatabasesAsync_CodeScript()
        {
            var clusters = new ClusterSymbol[]
            {
                new ClusterSymbol("cluster1.kusto.windows.net",
                    new DatabaseSymbol("db1",
                        new TableSymbol("Table1", "(x: long)")),
                    new DatabaseSymbol("db2",
                        new TableSymbol("Table2", "(x: long)")))
            };

            var loader = new TestLoader(clusters, "cluster1");
            var resolver = new SymbolResolver(loader);

            // set default database.
            var globals = await loader.AddOrUpdateDefaultDatabaseAsync(GlobalState.Default, "db1");

            // just one database should exist
            Assert.AreEqual(1, globals.Cluster.Databases.Count);

            // create script from query and globals
            var script = CodeScript.From("database('db2').Table2", globals);

            // use loader to add symbols for any explicity referenced databases
            var newScript = await resolver.AddReferencedDatabasesAsync(script);

            // both databases should exist now
            Assert.AreEqual(2, newScript.Globals.Cluster.Databases.Count);

            // find table in database
            var db2 = newScript.Globals.Cluster.GetDatabase("db2");
            var table2 = db2.GetTable("Table2");
        }

        [TestMethod]
        public async Task TestAddReferencedDatabasesAsync_ReferenceInFunction()
        {
            var clusters = new ClusterSymbol[]
            {
                new ClusterSymbol("cluster1.kusto.windows.net",
                    new DatabaseSymbol("db1",
                        new FunctionSymbol("Fn1", "{ database('db2').Table2 } ")),
                    new DatabaseSymbol("db2",
                        new TableSymbol("Table2", "(x: long)")))
            };

            var loader = new TestLoader(clusters, "cluster1");
            var resolver = new SymbolResolver(loader);

            // set default database.
            var globals = await loader.AddOrUpdateDefaultDatabaseAsync(GlobalState.Default, "db1");

            // just one database should exist
            Assert.AreEqual(1, globals.Cluster.Databases.Count);

            // parse query that has reference to other database inside function body
            var code = KustoCode.ParseAndAnalyze("Fn1()", globals);

            // use loader to add symbols for any explicity referenced databases
            var newCode = await resolver.AddReferencedDatabasesAsync(code);

            // both databases should exist now
            Assert.AreEqual(2, newCode.Globals.Cluster.Databases.Count);

            // find table in database
            var db2 = newCode.Globals.Cluster.GetDatabase("db2");
            var table2 = db2.GetTable("Table2");

            // verify reference to Table2 actually is Table2
            var expr = newCode.Syntax.GetFirstDescendant<FunctionCallExpression>();
            Assert.AreSame(table2, expr.ResultType);
        }

        [TestMethod]
        public async Task TestAddReferencedDatabasesAsync_MultipleHops()
        {
            var clusters = new ClusterSymbol[]
            {
                new ClusterSymbol("cluster1.kusto.windows.net",
                    new DatabaseSymbol("db1",
                        new FunctionSymbol("Fn1", "{ database('db2').Fn2() }")),
                    new DatabaseSymbol("db2",
                        new FunctionSymbol("Fn2", "{ database('db3').Table3 }")),
                    new DatabaseSymbol("db3",
                        new TableSymbol("Table3", "(x: long)")))                      
            };

            var loader = new TestLoader(clusters, "cluster1");
            var resolver = new SymbolResolver(loader);

            // set default database.
            var globals = await loader.AddOrUpdateDefaultDatabaseAsync(GlobalState.Default, "db1");

            // just one database should exist at this time
            Assert.AreEqual(1, globals.Cluster.Databases.Count);

            // parse query that has reference to other database inside function body
            var code = KustoCode.ParseAndAnalyze("Fn1()", globals);

            // use loader to add symbols for any explicity referenced databases
            var newCode = await resolver.AddReferencedDatabasesAsync(code);

            // all three databases should exist now
            Assert.AreEqual(3, newCode.Globals.Cluster.Databases.Count);

            // find table in database
            var db3 = newCode.Globals.Cluster.GetDatabase("db3");
            var table3 = db3.GetTable("Table3");

            // verify reference to table
            var expr = newCode.Syntax.GetFirstDescendant<FunctionCallExpression>();
            Assert.AreSame(table3, expr.ResultType);
        }

        [TestMethod]
        public async Task TestAddReferencedDatabasesAsync_CodeScript_MultipleHops()
        {
            var clusters = new ClusterSymbol[]
            {
                new ClusterSymbol("cluster1.kusto.windows.net",
                    new DatabaseSymbol("db1",
                        new FunctionSymbol("Fn1", "{ database('db2').Fn2() }")),
                    new DatabaseSymbol("db2",
                        new FunctionSymbol("Fn2", "{ database('db3').Table3 }")),
                    new DatabaseSymbol("db3",
                        new TableSymbol("Table3", "(x: long)")))
            };

            var loader = new TestLoader(clusters, "cluster1");
            var resolver = new SymbolResolver(loader);

            // set default database.
            var globals = await loader.AddOrUpdateDefaultDatabaseAsync(GlobalState.Default, "db1");

            // just one database should exist at this time
            Assert.AreEqual(1, globals.Cluster.Databases.Count);

            // create script that has query with call to function
            var script = CodeScript.From("Fn1()", globals);

            // use loader to add symbols for any explicity referenced databases
            var newScript = await resolver.AddReferencedDatabasesAsync(script);

            // all three databases should exist now
            Assert.AreEqual(3, newScript.Globals.Cluster.Databases.Count);
        }

        [TestMethod]
        public async Task TestAddReferencedDatabasesAsync_MultipleClusters()
        {
            var clusters = new ClusterSymbol[]
            {
                new ClusterSymbol("cluster1.kusto.windows.net",
                    new DatabaseSymbol("db1",
                        new TableSymbol("Table1", "(x: long)"))),
                new ClusterSymbol("cluster2.kusto.windows.net",
                    new DatabaseSymbol("db2",
                        new TableSymbol("Table2", "(x: long)")))
            };

            var loader = new TestLoader(clusters, "cluster1");
            var resolver = new SymbolResolver(loader);

            // set default database.
            var globals = await loader.AddOrUpdateDefaultDatabaseAsync(GlobalState.Default, "db1");

            // just one cluster should exist
            Assert.AreEqual(1, globals.Clusters.Count);

            // parse query that has explicit reference to other database
            var code = KustoCode.ParseAndAnalyze("cluster('cluster2').database('db2').Table2", globals);

            // use loader to add symbols for any explicity referenced databases
            var newCode = await resolver.AddReferencedDatabasesAsync(code);

            // both clusters should exist
            Assert.AreEqual(2, newCode.Globals.Clusters.Count);

            // find table in database
            var cluster2 = newCode.Globals.GetCluster("cluster2");
            var db2 = cluster2.GetDatabase("db2");
            var table2 = db2.GetTable("Table2");

            // verify reference to table
            var expr = newCode.Syntax.GetFirstDescendant<NameReference>(nr => nr.SimpleName == "Table2");
            Assert.AreSame(table2, expr.ResultType);
        }

        [TestMethod]
        public async Task TestAddReferencedDatabasesAsync_MultipleHops_MultipleClusters()
        {
            var clusters = new ClusterSymbol[]
            {
                new ClusterSymbol("cluster1.kusto.windows.net",
                    new DatabaseSymbol("db1",
                        new FunctionSymbol("Fn1", "{ cluster('cluster2').database('db2').Fn2() }"))),
                new ClusterSymbol("cluster2.kusto.windows.net",
                    new DatabaseSymbol("db2",
                        new FunctionSymbol("Fn2", "{ cluster('cluster3').database('db3').Table3 }"))),
                new ClusterSymbol("cluster3.kusto.windows.net",
                    new DatabaseSymbol("db3",
                        new TableSymbol("Table3", "(x: long)")))
            };

            var loader = new TestLoader(clusters, "cluster1");
            var resolver = new SymbolResolver(loader);

            // set default database.
            var globals = await loader.AddOrUpdateDefaultDatabaseAsync(GlobalState.Default, "db1");

            // just one cluster should exist at this time
            Assert.AreEqual(1, globals.Clusters.Count);

            // parse query that has reference to other database inside function body
            var code = KustoCode.ParseAndAnalyze("Fn1()", globals);

            // use loader to add symbols for any explicity referenced databases
            var newCode = await resolver.AddReferencedDatabasesAsync(code);

            // all three clusters should exist now
            Assert.AreEqual(3, newCode.Globals.Clusters.Count);

            // find table in database
            var cluster3 = newCode.Globals.GetCluster("cluster3");
            var db3 = cluster3.GetDatabase("db3");
            var table3 = db3.GetTable("Table3");

            // verify reference to table
            var expr = newCode.Syntax.GetFirstDescendant<FunctionCallExpression>();
            Assert.AreSame(table3, expr.ResultType);
        }
    }
}
