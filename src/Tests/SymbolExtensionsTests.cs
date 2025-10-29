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
    public class SymbolExtensionsTests
    {
        private GlobalState _globals = GlobalState.Default
            .WithClusterList([
                new ClusterSymbol("cluster",
                    new DatabaseSymbol("db1",
                        new TableSymbol("T1", "(x: long)"),
                        new TableSymbol("T2", "(y: string)"),
                        new TableSymbol("Tee Three", "(z: string)"),
                        new ExternalTableSymbol("ET", "(x: long)"),
                        new MaterializedViewSymbol("MV", "(y: string)", "T1"),
                        new FunctionSymbol("F1", "()", "{ T1 }")
                        ),
                    new DatabaseSymbol("db2",
                        new TableSymbol("T1", "(x: long)"),
                        new TableSymbol("T2", "(y: string)"),
                        new ExternalTableSymbol("ET", "(x: long)"),
                        new MaterializedViewSymbol("MV", "(y: string)", "T1"),
                        new FunctionSymbol("F1", "()", "{ T1 }")
                    )),
                new ClusterSymbol("cluster2",
                    new DatabaseSymbol("db3",
                        new TableSymbol("T1", "(x: long)"),
                        new TableSymbol("T2", "(y: string)"),
                        new ExternalTableSymbol("ET", "(x: long)"),
                        new MaterializedViewSymbol("MV", "(y: string)", "T1"),
                        new FunctionSymbol("F1", "()", "{ T1 }")
                    ))
                ])
            .WithCluster("cluster")
            .WithDatabase("db1");

        [TestMethod]
        public void TestTryGetDatabase()
        {
            TestTryGetDatabase(_globals.Cluster.GetDatabase("db1").GetTable("T1"), "db1");
            TestTryGetDatabase(_globals.Cluster.GetDatabase("db2").GetFunction("F1"), "db2");
        }

        private void TestTryGetDatabase(Symbol symbol, string expectedDatabaseName)
        {
            Assert.IsTrue(SymbolExtensions.TryGetDatabase(symbol, _globals, out var db));
            Assert.IsNotNull(db);
            Assert.AreEqual(expectedDatabaseName, db.Name);
        }

        [TestMethod]
        public void TestTryGetCluster()
        {
            TryGetCluster(_globals.Cluster.GetDatabase("db1"), "cluster");
        }

        private void TryGetCluster(DatabaseSymbol database, string expectedClusterName)
        {
            Assert.IsTrue(database.TryGetCluster(_globals, out var actualCluster));
            Assert.IsNotNull(actualCluster);
            Assert.AreEqual(expectedClusterName, actualCluster.Name);
        }

        [TestMethod]
        public void TestTryGetClusterAndDatabase()
        {
            TestTryGetClusterAndDatabase(_globals.Cluster.GetDatabase("db1").GetTable("T1"), "cluster", "db1");
            TestTryGetClusterAndDatabase(_globals.Cluster.GetDatabase("db2").GetFunction("F1"), "cluster", "db2");
        }

        private void TestTryGetClusterAndDatabase(Symbol symbol, string expectedClusterName, string expectedDatabaseName)
        {
            Assert.IsTrue(SymbolExtensions.TryGetClusterAndDatabase(symbol, _globals, out var actualCluster, out var actualDatabase));
            Assert.IsNotNull(actualCluster);
            Assert.IsNotNull(actualDatabase);
            Assert.AreEqual(expectedClusterName, actualCluster.Name);
            Assert.AreEqual(expectedDatabaseName, actualDatabase.Name);
        }

        [TestMethod]
        public void TestTryGetTable()
        {
            TestTryGetTable(_globals.Cluster.GetDatabase("db1").GetTable("T1").GetColumn("x"), "T1");
        }

        private void TestTryGetTable(ColumnSymbol column, string expectedTableName)
        {
            Assert.IsTrue(column.TryGetTable(_globals, out var table));
            Assert.IsNotNull(table);
            Assert.AreEqual(expectedTableName, table.Name);
        }

        [TestMethod]
        public void TestTryGetDatabaseAndTable()
        {
            TestTryGetDatabaseAndTable(_globals.Cluster.GetDatabase("db1").GetTable("T1").GetColumn("x"), "db1", "T1");
        }

        private void TestTryGetDatabaseAndTable(ColumnSymbol column, string expectedDatabaseName, string expectedTableName)
        {
            Assert.IsTrue(column.TryGetDatabaseAndTable(_globals, out var database, out var table));
            Assert.IsNotNull(database);
            Assert.IsNotNull(table);
            Assert.AreEqual(expectedDatabaseName, database.Name);
            Assert.AreEqual(expectedTableName, table.Name);
        }

        [TestMethod]
        public void TestTryGetClusterDatabaseAndTable()
        {
            TestTryGetClusterDatabaseAndTable(_globals.Cluster.GetDatabase("db1").GetTable("T1").GetColumn("x"), "cluster", "db1", "T1");
        }

        private void TestTryGetClusterDatabaseAndTable(ColumnSymbol column, string expectedClusterName, string expectedDatabaseName, string expectedTableName)
        {
            Assert.IsTrue(column.TryGetClusterDatabaseAndTable(_globals, out var cluster, out var database, out var table));
            Assert.IsNotNull(cluster);
            Assert.IsNotNull(database);
            Assert.IsNotNull(table);
            Assert.AreEqual(expectedClusterName, cluster.Name);
            Assert.AreEqual(expectedDatabaseName, database.Name);
            Assert.AreEqual(expectedTableName, table.Name);
        }

        [TestMethod]
        public void TestGetMinimalExpression()
        {
            TestGetMinimalExpression(_globals.Cluster, "cluster('cluster')");
            TestGetMinimalExpression(_globals.Cluster.GetDatabase("db1"), "database('db1')");
            TestGetMinimalExpression(_globals.GetCluster("cluster2").GetDatabase("db3"), "cluster('cluster2').database('db3')");
            TestGetMinimalExpression(_globals.Cluster.GetDatabase("db1").GetTable("T1"), "T1");
            TestGetMinimalExpression(_globals.Cluster.GetDatabase("db2").GetTable("T1"), "database('db2').T1");
            TestGetMinimalExpression(_globals.GetCluster("cluster2").GetDatabase("db3").GetTable("T1"), "cluster('cluster2').database('db3').T1");
            TestGetMinimalExpression(_globals.Database.GetTable("Tee Three"), "['Tee Three']");
            TestGetMinimalExpression(_globals.Database.GetTable("T1").GetColumn("x"), "x");
        }

        private void TestGetMinimalExpression(Symbol symbol, string expected)
        {
            Assert.IsNotNull(symbol);
            var actual = symbol.GetMinimalExpression(_globals);
            Assert.AreEqual(expected, actual);
        }

        [TestMethod]
        public void TestGetExpression()
        {
            TestGetExpression(_globals.Cluster, "cluster('cluster')");
            TestGetExpression(_globals.Cluster.GetDatabase("db1"), "cluster('cluster').database('db1')");
            TestGetExpression(_globals.GetCluster("cluster2").GetDatabase("db3"), "cluster('cluster2').database('db3')");
            TestGetExpression(_globals.Cluster.GetDatabase("db1").GetTable("T1"), "cluster('cluster').database('db1').T1");
            TestGetExpression(_globals.Cluster.GetDatabase("db2").GetTable("T1"), "cluster('cluster').database('db2').T1");
            TestGetExpression(_globals.GetCluster("cluster2").GetDatabase("db3").GetTable("T1"), "cluster('cluster2').database('db3').T1");
            TestGetExpression(_globals.Database.GetTable("Tee Three"), "cluster('cluster').database('db1').['Tee Three']");
            TestGetExpression(_globals.Database.GetTable("T1").GetColumn("x"), "x");
        }

        private void TestGetExpression(Symbol symbol, string expected)
        {
            Assert.IsNotNull(symbol);
            var actual = symbol.GetExpression(_globals);
            Assert.AreEqual(expected, actual);
        }
    }
}
