using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Kusto.Language;
using Kusto.Toolkit;
using Kusto.Language.Symbols;

namespace Tests
{
    [TestClass]
    public class KustoExtensionsTests
    {
        [TestMethod]
        public void TestGetDatabaseTables()
        {
            var globals = GlobalState.Default
                .WithCluster(
                    new ClusterSymbol("cluster",
                        new DatabaseSymbol("db1",
                            new TableSymbol("Tab11", "(x: long)"),
                            new TableSymbol("Tab12", "(y: string)")),
                        new DatabaseSymbol("db2",
                            new TableSymbol("Tab21", "(x: long)"),
                            new TableSymbol("Tab22", "(y: string)"))))
                .WithDatabase("db1");

            TestGetDatabaseTables("Tab11", "db1.Tab11", globals);
            TestGetDatabaseTables("Tab12", "db1.Tab12", globals);
            TestGetDatabaseTables("database('db2').Tab21", "db2.Tab21", globals);
            TestGetDatabaseTables("database('db2').Tab22", "db2.Tab22", globals);
            TestGetDatabaseTables("Tab11 | union Tab12", "db1.Tab11, db1.Tab12", globals);
            TestGetDatabaseTables("Tab11 | union database('db2').Tab22", "db1.Tab11, db2.Tab22", globals);
            TestGetDatabaseTables("union Tab*", "db1.Tab11, db1.Tab12", globals);
        }

        [TestMethod]
        public void TestGetDatabaseTables_macro_expand()
        {
            var globals = GlobalState.Default
                .WithClusterList(
                    new ClusterSymbol("cluster",
                        new DatabaseSymbol("db",
                            new EntityGroupSymbol("ClusterGroup", "[cluster('cluster1'), cluster('cluster2')]")
                            )),
                    new ClusterSymbol("cluster1",
                        new DatabaseSymbol("db1",
                            new TableSymbol("Tab11", "(x: long)"),
                            new TableSymbol("Tab12", "(y: string)")),
                        new DatabaseSymbol("db2",
                            new TableSymbol("Tab21", "(x: long)"),
                            new TableSymbol("Tab22", "(y: string)"))),
                    new ClusterSymbol("cluster2",
                        new DatabaseSymbol("db1",
                            new TableSymbol("Tab11", "(x: long)"),
                            new TableSymbol("Tab12", "(y: string)")),
                        new DatabaseSymbol("db2",
                            new TableSymbol("Tab21", "(x: long)"),
                            new TableSymbol("Tab22", "(y: string)")))
                    )
                .WithCluster("cluster")
                .WithDatabase("db");

            TestGetDatabaseTables("macro-expand ClusterGroup as scope (scope.database('db1').Tab11)", "cluster1.db1.Tab11, cluster2.db1.Tab11", globals);
            TestGetDatabaseTables("macro-expand ClusterGroup as scope (scope.database('db1').Tab12)", "cluster1.db1.Tab12, cluster2.db1.Tab12", globals);
            TestGetDatabaseTables("macro-expand ClusterGroup as scope (scope.database('db2').Tab21)", "cluster1.db2.Tab21, cluster2.db2.Tab21", globals);
            TestGetDatabaseTables("macro-expand ClusterGroup as scope (scope.database('db2').Tab22)", "cluster1.db2.Tab22, cluster2.db2.Tab22", globals);
            TestGetDatabaseTables("macro-expand ClusterGroup as scope (scope.database('db1').Tab11 | union scope.database('db1').Tab12)", "cluster1.db1.Tab11, cluster1.db1.Tab12, cluster2.db1.Tab11, cluster2.db1.Tab12", globals);
            TestGetDatabaseTables("macro-expand ClusterGroup as scope (scope.database('db1').Tab11 | union scope.database('db2').Tab22)", "cluster1.db1.Tab11, cluster1.db2.Tab22, cluster2.db1.Tab11, cluster2.db2.Tab22", globals);
            TestGetDatabaseTables("macro-expand ClusterGroup as scope (union scope.database('db1').Tab*)", "cluster1.db1.Tab11, cluster1.db1.Tab12, cluster2.db1.Tab11, cluster2.db1.Tab12", globals);
        }

        [TestMethod]
        public void TestGetDatabaseTables_functions()
        {
            var globals = GlobalState.Default
                .WithCluster(
                    new ClusterSymbol("cluster",
                        new DatabaseSymbol("db1",
                            new TableSymbol("Tab11", "(x: long)"),
                            new TableSymbol("Tab12", "(y: string)"),
                            new FunctionSymbol("Fn_11_21", "()", "{ Tab11 | union database('db2').Tab21 }"),
                            new FunctionSymbol("Fn_11_22_12", "()", "{ Tab11 | union database('db2').Fn_22_12 }")),
                        new DatabaseSymbol("db2",
                            new TableSymbol("Tab21", "(x: long)"),
                            new TableSymbol("Tab22", "(y: string)"),
                            new FunctionSymbol("Fn_22_12", "()", "{ Tab22 | union database('db1').Tab12 }"),
                            new FunctionSymbol("Fn_21_11_22_12", "()", "{ Tab21 | union database('db1').Fn_11_22_12 }"))))
                .WithDatabase("db1");

            TestGetDatabaseTables("Fn_11_21", "db1.Tab11, db2.Tab21", globals);
            TestGetDatabaseTables("Fn_11_22_12", "db1.Tab11, db2.Tab22, db1.Tab12", globals);
            TestGetDatabaseTables("database('db2').Fn_21_11_22_12", "db2.Tab21, db1.Tab11, db2.Tab22, db1.Tab12", globals);
            //TestGetDatabaseTables("union Fn_11*", "db1.Tab11, db2.Tab22, db1.Tab12", globals);
        }

        private static void TestGetDatabaseTables(string query, string tableNames, GlobalState globals)
        {
            var code = KustoCode.ParseAndAnalyze(query, globals);
            var dx = code.GetDiagnostics();
            if (dx.Count > 0)
            {
                Assert.Fail($"unexpected diagnostic: {dx[0].Message}");
            }

            var expectedTables = GetTables(tableNames, globals);
            var actualTables = code.GetDatabaseTablesReferenced();

            var expectedDottedNames = expectedTables.Select(t => GetDottedName(t, globals)).OrderBy(x => x).ToArray();
            var actualDottedNames = actualTables.Select(t => GetDottedName(t, globals)).OrderBy(x => x).ToArray();

            var expectedTableNames = string.Join(", ", expectedDottedNames);
            var actualTableNames = string.Join(", ", actualDottedNames);

            Assert.AreEqual(expectedTableNames, actualTableNames);
        }

        [TestMethod]
        public void TestGetDatabaseTableColumns()
        {
            var globals = GlobalState.Default
                .WithCluster(
                    new ClusterSymbol("cluster",
                        new DatabaseSymbol("db1",
                            new TableSymbol("Tab11", "(x: long, y: string)"),
                            new TableSymbol("Tab12", "(a: long, b: real)")),
                        new DatabaseSymbol("db2",
                            new TableSymbol("Tab21", "(x: long, y: string)"),
                            new TableSymbol("Tab22", "(a: long, b: string)"))))
                .WithDatabase("db1");

            TestGetDatabaseTableColumns("Tab11", "", globals);
            TestGetDatabaseTableColumns("Tab11 | where x > 10", "Tab11.x", globals);
            TestGetDatabaseTableColumns("Tab11 | join Tab12 on $left.x == $right.a", "Tab11.x, Tab12.a", globals);
            TestGetDatabaseTableColumns("Tab11 | union database('db2').Tab21 | project x", "Tab11.x, db2.Tab21.x", globals);
        }

        [TestMethod]
        public void TestGetDatabaseTableColumns_macro_expand()
        {
            var globals = GlobalState.Default
                .WithClusterList(
                    new ClusterSymbol("cluster",
                        new DatabaseSymbol("db",
                            new EntityGroupSymbol("ClusterGroup", "[cluster('cluster1'), cluster('cluster2')]"),
                            new EntityGroupSymbol("DbGroup", "[cluster('cluster1').database('db1'), cluster('cluster1').database('db2'), cluster('cluster2').database('db1'), cluster('cluster2').database('db2')]")
                            )),
                    new ClusterSymbol("cluster1",
                        new DatabaseSymbol("db1",
                            new TableSymbol("T", "(x: long, y: string)")),
                        new DatabaseSymbol("db2",
                            new TableSymbol("T", "(x: long, y: string)"))),
                    new ClusterSymbol("cluster2",
                        new DatabaseSymbol("db1",
                            new TableSymbol("T", "(x: long, y: string)")),
                        new DatabaseSymbol("db2",
                            new TableSymbol("T", "(x: long, y: string)")))
                    )
                .WithCluster("cluster")
                .WithDatabase("db");

            TestGetDatabaseTableColumns("macro-expand ClusterGroup as scope (scope.database('db1').T | project x)", "cluster1.db1.T.x, cluster2.db1.T.x", globals);
            TestGetDatabaseTableColumns("macro-expand DbGroup as scope (scope.T | project x)", "cluster1.db1.T.x, cluster1.db2.T.x, cluster2.db1.T.x, cluster2.db2.T.x", globals);
        }

        [TestMethod]
        public void TestGetDatabaseTableColumns_functions()
        {
            var globals = GlobalState.Default
                .WithCluster(
                    new ClusterSymbol("cluster",
                        new DatabaseSymbol("db1",
                            new TableSymbol("Tab11", "(x: long, y: string)"),
                            new TableSymbol("Tab12", "(a: long, b: real)"),
                            new FunctionSymbol("Fn_11_21", "()", "{ Tab11 | union database('db2').Tab21 | where x > 10 }"),
                            new FunctionSymbol("Fn_11_22_12", "()", "{ Tab11 | union database('db2').Fn_22_12 | where x > a }")),
                        new DatabaseSymbol("db2",
                            new TableSymbol("Tab21", "(x: long, y: string)"),
                            new TableSymbol("Tab22", "(a: long, y: real)"),
                            new FunctionSymbol("Fn_22_12", "()", "{ Tab22 | union database('db1').Tab12 | where a > 10 }"),
                            new FunctionSymbol("Fn_21_11_22_12", "()", "{ Tab21 | union database('db1').Fn_11_22_12 | where x > a }"))))
                .WithDatabase("db1");

            TestGetDatabaseTableColumns("Fn_11_21", "db1.Tab11.x, db2.Tab21.x", globals);
            TestGetDatabaseTableColumns("Fn_11_22_12", "db2.Tab22.a, db1.Tab12.a, db1.Tab11.x", globals);
            TestGetDatabaseTableColumns("database('db2').Fn_22_12", "db2.Tab22.a, db1.Tab12.a", globals);
            TestGetDatabaseTableColumns("database('db2').Fn_21_11_22_12", "db1.Tab11.x, db2.Tab21.x, db2.Tab22.a, db1.Tab12.a", globals);
        }

        private static void TestGetDatabaseTableColumns(string query, string columnNames, GlobalState globals)
        {
            TestGetColumns(query, columnNames, globals, code => code.GetDatabaseTableColumnsReferenced());
        }

        [TestMethod]
        public void TestGetDatabaseTableColumnsInResult()
        {
            var globals = GlobalState.Default
                .WithCluster(
                    new ClusterSymbol("cluster",
                        new DatabaseSymbol("db1",
                            new TableSymbol("Tab11", "(x: long, y: string)"),
                            new TableSymbol("Tab12", "(a: long, b: real)")),
                        new DatabaseSymbol("db2",
                            new TableSymbol("Tab21", "(x: long, y: string)"),
                            new TableSymbol("Tab22", "(a: long, y: real)"))))
                .WithDatabase("db1");

            TestGetDatabaseTableColumnsInResult("Tab11", "Tab11.x, Tab11.y", globals);
            TestGetDatabaseTableColumnsInResult("Tab11 | union Tab12", "Tab11.x, Tab11.y, Tab12.a, Tab12.b", globals);
            TestGetDatabaseTableColumnsInResult("Tab11 | project x", "Tab11.x", globals);
            TestGetDatabaseTableColumnsInResult("Tab11 | project Q=x", "", globals);
        }

        [TestMethod]
        public void TestGetDatabaseTableColumnsInResult_macro_expand()
        {
            var globals = GlobalState.Default
                .WithClusterList(
                    new ClusterSymbol("cluster",
                        new DatabaseSymbol("db",
                            new EntityGroupSymbol("Db1Group", "[cluster('cluster1').database('db1'), cluster('cluster2').database('db1')]")
                            )),
                    new ClusterSymbol("cluster1",
                        new DatabaseSymbol("db1",
                            new TableSymbol("Tab11", "(x: long, y: string)"),
                            new TableSymbol("Tab12", "(a: long, b: real)")),
                        new DatabaseSymbol("db2",
                            new TableSymbol("Tab21", "(x: long, y: string)"),
                            new TableSymbol("Tab22", "(a: long, y: real)"))),
                    new ClusterSymbol("cluster2",
                        new DatabaseSymbol("db1",
                            new TableSymbol("Tab11", "(x: long, y: string)"),
                            new TableSymbol("Tab12", "(a: long, b: real)")),
                        new DatabaseSymbol("db2",
                            new TableSymbol("Tab21", "(x: long, y: string)"),
                            new TableSymbol("Tab22", "(a: long, y: real)")))
                    )
                .WithCluster("cluster")
                .WithDatabase("db");

            TestGetDatabaseTableColumnsInResult(
                "macro-expand Db1Group as scope (scope.Tab11)", 
                "cluster1.db1.Tab11.x, cluster1.db1.Tab11.y, cluster2.db1.Tab11.x, cluster2.db1.Tab11.y", 
                globals);

            TestGetDatabaseTableColumnsInResult(
                "macro-expand Db1Group as scope (scope.Tab11 | union scope.Tab12)", 
                "cluster1.db1.Tab11.x, cluster1.db1.Tab11.y, cluster1.db1.Tab12.a, cluster1.db1.Tab12.b, " +
                "cluster2.db1.Tab11.x, cluster2.db1.Tab11.y, cluster2.db1.Tab12.a, cluster2.db1.Tab12.b", 
                globals);
            
            TestGetDatabaseTableColumnsInResult(
                "macro-expand Db1Group as scope (scope.Tab11 | project x)", 
                "cluster1.db1.Tab11.x, cluster2.db1.Tab11.x", 
                globals);

            TestGetDatabaseTableColumnsInResult(
                "macro-expand Db1Group as scope (scope.Tab11 | project Q=x)", 
                "", 
                globals);
        }

        private static void TestGetDatabaseTableColumnsInResult(string query, string columnNames, GlobalState globals)
        {
            TestGetColumns(query, columnNames, globals, code => code.GetDatabaseTableColumnsInResult());
        }

        [TestMethod]
        public void TestGetSourceColumns()
        {
            var globals = GlobalState.Default
                .WithCluster(
                    new ClusterSymbol("cluster",
                        new DatabaseSymbol("db1",
                            new TableSymbol("TabXY", "(x: long, y: string)"),
                            new TableSymbol("TabXZ", "(x: long, z: real)"),
                            new FunctionSymbol("FnQ", "()", "{ TabXY | project Q=x }"),
                            new FunctionSymbol("FnPQ", "()", "{ FnQ | project P=Q }"))))
                .WithDatabase("db1");

            // table's columns are their own source
            TestGetSourceColumns("TabXY", "TabXY.x, TabXY.y", globals);
            TestGetSourceColumns("TabXY", "x", "TabXY.x", globals);
            TestGetSourceColumns("TabXY", "y", "TabXY.y", globals);

            // projected table column is its own source
            TestGetSourceColumns("TabXY | project x", "x", "TabXY.x", globals);
            TestGetSourceColumns("TabXY | project x, y", "x", "TabXY.x", globals);
            TestGetSourceColumns("TabXY | project x, y", "y", "TabXY.y", globals);

            // projected and renamed column can be tracked back to its source
            TestGetSourceColumns("TabXY | project Q=x", "Q", "TabXY.x", globals);

            // unioned and renamed columns can be tracked back to their source
            TestGetSourceColumns("TabXY | union TabXZ | project x", "x", "TabXY.x, TabXZ.x", globals);
            TestGetSourceColumns("TabXY | union TabXZ | project Q=x", "Q", "TabXY.x, TabXZ.x", globals);

            // joined columns can be tracked back to their source
            TestGetSourceColumns("TabXY | join TabXZ on x | project Q=x", "Q", "TabXY.x", globals);
            TestGetSourceColumns("TabXY | join TabXZ on x | project x1", "x1", "TabXZ.x", globals);

            // columns generated by aggregates can be tracked back to their source
            TestGetSourceColumns("TabXY | summarize max(x), min(x)", "max_x", "TabXY.x", globals);
            TestGetSourceColumns("TabXY | summarize max(x), min(x)", "min_x", "TabXY.x", globals);
            TestGetSourceColumns("TabXY | summarize max(x), min(y)", "max_x", "TabXY.x", globals);
            TestGetSourceColumns("TabXY | summarize max(x), min(y)", "min_y", "TabXY.y", globals);
            TestGetSourceColumns("TabXY | summarize min(Q=x)", "TabXY.x", globals);

            // columns referenced and renamed inside functions can be tracked back to their source
            TestGetSourceColumns("FnQ", "Q", "TabXY.x", globals);
            TestGetSourceColumns("FnPQ", "P", "TabXY.x", globals);

            // columns generated within local functions can be tracked back to their source
            TestGetSourceColumns("let fn=() { TabXY | project P=x, Q=y }; fn()", "P", "TabXY.x", globals);
            TestGetSourceColumns("let fn=() { TabXY | project P=x, Q=y }; fn()", "Q", "TabXY.y", globals);

            // columns passed as arguments can be tracked
            TestGetSourceColumns("let fn=(text: string) { text }; TabXY | project fn(y)", "Column1", "TabXY.y", globals);
            TestGetSourceColumns("let fn=(text: string) { text }; TabXY | project Q=fn(y)", "Q", "TabXY.y", globals);
        }

        [TestMethod]
        public void TestGetSourceColumns_macro_expand()
        {
            var globals = GlobalState.Default
                .WithClusterList(
                    new ClusterSymbol("cluster",
                        new DatabaseSymbol("db",
                            new EntityGroupSymbol("Db1Group", "[cluster('cluster1').database('db1'), cluster('cluster2').database('db1')]"))),
                    new ClusterSymbol("cluster1",
                        new DatabaseSymbol("db1",
                            new TableSymbol("TabXY", "(x: long, y: string)"),
                            new TableSymbol("TabXZ", "(x: long, z: real)"),
                            new FunctionSymbol("FnQ", "()", "{ TabXY | project Q=x }"),
                            new FunctionSymbol("FnPQ", "()", "{ FnQ | project P=Q }"))),
                    new ClusterSymbol("cluster2",
                        new DatabaseSymbol("db1",
                            new TableSymbol("TabXY", "(x: long, y: string)"),
                            new TableSymbol("TabXZ", "(x: long, z: real)"),
                            new FunctionSymbol("FnQ", "()", "{ TabXY | project Q=x }"),
                            new FunctionSymbol("FnPQ", "()", "{ FnQ | project P=Q }")))
                    )
                .WithCluster("cluster")
                .WithDatabase("db");

            // table's columns are their own source
            TestGetSourceColumns(
                "macro-expand Db1Group as scope (scope.TabXY)", 
                "cluster1.db1.TabXY.x, cluster1.db1.TabXY.y, cluster2.db1.TabXY.x, cluster2.db1.TabXY.y",
                globals);

            TestGetSourceColumns(
                "macro-expand Db1Group as scope (scope.TabXY)", 
                "x", 
                "cluster1.db1.TabXY.x, cluster2.db1.TabXY.x",
                globals);

            TestGetSourceColumns(
                "macro-expand Db1Group as scope (scope.TabXY)", 
                "y", 
                "cluster1.db1.TabXY.y, cluster2.db1.TabXY.y",
                globals);

            // projected table column is its own source
            TestGetSourceColumns(
                "macro-expand Db1Group as scope (scope.TabXY | project x)", 
                "x", 
                "cluster1.db1.TabXY.x, cluster2.db1.TabXY.x", 
                globals);

            TestGetSourceColumns(
                "macro-expand Db1Group as scope (scope.TabXY | project x, y)", 
                "x", 
                "cluster1.db1.TabXY.x, cluster2.db1.TabXY.x", 
                globals);

            TestGetSourceColumns(
                "macro-expand Db1Group as scope (scope.TabXY | project x, y)", 
                "y", 
                "cluster1.db1.TabXY.y, cluster2.db1.TabXY.y", 
                globals);

            // projected and renamed column can be tracked back to its source
            TestGetSourceColumns(
                "macro-expand Db1Group as scope (scope.TabXY | project Q=x)", 
                "Q", 
                "cluster1.db1.TabXY.x, cluster2.db1.TabXY.x", 
                globals);

            // unioned and renamed columns can be tracked back to their source
            TestGetSourceColumns(
                "macro-expand Db1Group as scope (scope.TabXY | union scope.TabXZ | project x)", 
                "x",
                "cluster1.db1.TabXY.x, cluster1.db1.TabXZ.x, cluster2.db1.TabXY.x, cluster2.db1.TabXZ.x", 
                globals);

            TestGetSourceColumns(
                "macro-expand Db1Group as scope (scope.TabXY | union scope.TabXZ | project Q=x)", 
                "Q",
                "cluster1.db1.TabXY.x, cluster1.db1.TabXZ.x, cluster2.db1.TabXY.x, cluster2.db1.TabXZ.x", 
                globals);

            // joined columns can be tracked back to their source
            TestGetSourceColumns(
                "macro-expand Db1Group as scope (scope.TabXY | join scope.TabXZ on x | project Q=x)", 
                "Q", 
                "cluster1.db1.TabXY.x, cluster2.db1.TabXY.x", 
                globals);

            TestGetSourceColumns(
                "macro-expand Db1Group as scope (scope.TabXY | join scope.TabXZ on x | project x1)", 
                "x1", 
                "cluster1.db1.TabXZ.x, cluster2.db1.TabXZ.x", 
                globals);

            // columns generated by aggregates can be tracked back to their source
            TestGetSourceColumns(
                "macro-expand Db1Group as scope (scope.TabXY | summarize max(x), min(x))", "max_x", 
                "cluster1.db1.TabXY.x, cluster2.db1.TabXY.x", 
                globals);

            TestGetSourceColumns(
                "macro-expand Db1Group as scope (scope.TabXY | summarize min(Q=x))", 
                "cluster1.db1.TabXY.x, cluster2.db1.TabXY.x", 
                globals);

            // columns referenced and renamed inside functions can be tracked back to their source
            TestGetSourceColumns(
                "macro-expand Db1Group as scope (scope.FnQ)", 
                "Q", 
                "cluster1.db1.TabXY.x, cluster2.db1.TabXY.x", 
                globals);

            TestGetSourceColumns(
                "macro-expand Db1Group as scope (scope.FnPQ)", 
                "P", 
                "cluster1.db1.TabXY.x, cluster2.db1.TabXY.x", 
                globals);
        }

        private static void TestGetSourceColumns(string query, string outputColumnName, string expectedSourceColumnNames, GlobalState globals)
        {
            TestGetColumns(query, expectedSourceColumnNames, globals, (code) =>
            {
                if (code.ResultType is TableSymbol tab
                    && tab.TryGetColumn(outputColumnName, out var column))
                {
                    return code.GetSourceColumns(column);
                }

                Assert.Fail($"Column '{outputColumnName}' not found in result");
                return null;
            });
        }

        private static void TestGetSourceColumns(string query, string expectedSourceColumnNames, GlobalState globals)
        {
            TestGetColumns(query, expectedSourceColumnNames, globals, code =>
            {
                return code.GetSourceColumns();
            });
        }

        private static void TestGetColumns(
            string query, string expectedSourceColumnNames, GlobalState globals, 
            Func<KustoCode, IReadOnlyList<ColumnSymbol>> fnGetColumns)
        {
            var code = KustoCode.ParseAndAnalyze(query, globals);
            var dx = code.GetDiagnostics();
            if (dx.Count > 0)
            {
                Assert.Fail($"unexpected diagnostic: {dx[0].Message}");
            }

            var expectedColumns = GetColumns(expectedSourceColumnNames, globals);
            var actualColumns = fnGetColumns(code);

            var expectedDottedNames = expectedColumns.Select(t => GetDottedName(t, globals)).OrderBy(x => x).ToArray();
            var actualDottedNames = actualColumns.Select(t => GetDottedName(t, globals)).OrderBy(x => x).ToArray();

            var expectedColumnNames = string.Join(", ", expectedDottedNames);
            var actualColumnNames = string.Join(", ", actualDottedNames);

            Assert.AreEqual(expectedColumnNames, actualColumnNames);
        }

        [TestMethod]
        public void TestGetDatabaseFunctions()
        {
            var globals = GlobalState.Default
                .WithCluster(
                    new ClusterSymbol("cluster",
                        new DatabaseSymbol("db",
                            new TableSymbol("T", "(x: long, y: string)"),
                            new FunctionSymbol("Fn1", "(a: long)", "{ T | where x == a }"),
                            new FunctionSymbol("Fn2", "(b: string)", "{ T | where y == b }"),
                            new FunctionSymbol("Fn3", "()", "{ Fn2('bbb') }")
                            )))
                .WithDatabase("db");

            TestGetStoredFunctions("Fn1(10)", "db.Fn1", globals);
            TestGetStoredFunctions("Fn2('bbb')", "db.Fn2", globals);
            TestGetStoredFunctions("Fn3()", "db.Fn2, db.Fn3", globals);
        }

        [TestMethod]
        public void TestGetDatabaseFunctions_macro_expand()
        {
            var globals = GlobalState.Default
                .WithCluster(
                    new ClusterSymbol("cluster",
                        new DatabaseSymbol("db",
                            new EntityGroupSymbol("DbGroup", "[database('db1'), database('db2')]")
                            ),
                        new DatabaseSymbol("db1",
                            new TableSymbol("T", "(x: long, y: string)"),
                            new FunctionSymbol("Fn", "(a: long)", "{ T | where x == a }")
                            ),
                        new DatabaseSymbol("db2",
                            new TableSymbol("T", "(x: long, y: string)"),
                            new FunctionSymbol("Fn", "(a: long)", "{ T | where x == a }")
                            )))
                .WithDatabase("db");

            TestGetStoredFunctions("macro-expand DbGroup as scope (scope.Fn(10))", "db1.Fn, db2.Fn", globals);
        }

        private static void TestGetStoredFunctions(string query, string functionNames, GlobalState globals)
        {
            var code = KustoCode.ParseAndAnalyze(query, globals);
            var dx = code.GetDiagnostics();
            if (dx.Count > 0)
            {
                Assert.Fail($"unexpected diagnostic: {dx[0].Message}");
            }

            var expectedFunctions = GetFunctions(functionNames, globals);
            var actualFunctions = code.GetStoredFunctionsReferenced();

            var expectedDottedNames = expectedFunctions.Select(t => GetDottedName(t, globals)).OrderBy(x => x).ToArray();
            var actualDottedNames = actualFunctions.Select(t => GetDottedName(t, globals)).OrderBy(x => x).ToArray();

            var expectedFunctionNames = string.Join(", ", expectedDottedNames);
            var actualFunctionNames = string.Join(", ", actualDottedNames);

            Assert.AreEqual(expectedFunctionNames, actualFunctionNames);
        }


        #region Test Helpers
        private static TableSymbol[] GetTables(string tableNames, GlobalState globals)
        {
            var qualifiedNames = ParseQualifiedNames(tableNames);
            return qualifiedNames.Select(qn => GetTable(qn, globals)).ToArray();
        }

        private static ColumnSymbol[] GetColumns(string columnNames, GlobalState globals)
        {
            var qualifiedNames = ParseColumnNames(columnNames);
            return qualifiedNames.Select(qn => GetColumn(qn, globals)).ToArray();
        }

        private static FunctionSymbol[] GetFunctions(string tableNames, GlobalState globals)
        {
            var qualifiedNames = ParseQualifiedNames(tableNames);
            return qualifiedNames.Select(qn => GetFunction(qn, globals)).ToArray();
        }

        private static string GetDottedName(DatabaseSymbol database, GlobalState globals)
        {
            var cluster = globals.GetCluster(database);
            if (cluster != null)
            {
                return $"{cluster.Name}.{database.Name}";
            }
            else
            {
                return database.Name;
            }
        }

        private static string GetDottedName(TableSymbol table, GlobalState globals)
        {
            var database = globals.GetDatabase(table);
            if (database != null)
            {
                return $"{GetDottedName(database, globals)}.{table.Name}";
            }
            else
            {
                return table.Name;
            }
        }

        private static string GetDottedName(ColumnSymbol column, GlobalState globals)
        {
            var table = globals.GetTable(column);
            if (table != null)
            {
                return $"{GetDottedName(table, globals)}.{column.Name}";
            }
            else
            {
                return column.Name;
            }
        }

        private static string GetDottedName(FunctionSymbol function, GlobalState globals)
        {
            var database = globals.GetDatabase(function);
            if (database != null)
            {
                return $"{GetDottedName(database, globals)}.{function.Name}";
            }
            else
            {
                return function.Name;
            }
        }

        private static QualifiedName[] ParseQualifiedNames(string tableNames)
        {
            return SplitNames(tableNames).Select(n => QualifiedName.ParseEntity(n)).ToArray();
        }

        private static QualifiedName[] ParseColumnNames(string columnNames)
        {
            return SplitNames(columnNames).Select(n => QualifiedName.ParseColumn(n)).ToArray();
        }

        private static readonly char[] s_NameSplitters = new char[] { ' ', ',' };
        private static string[] SplitNames(string names)
        {
            return names.Split(s_NameSplitters, StringSplitOptions.RemoveEmptyEntries);
        }

        private static ClusterSymbol GetCluster(QualifiedName name, GlobalState globals)
        {
            if (string.IsNullOrEmpty(name.ClusterName))
            {
                return globals.Cluster;
            }
            else
            {
                return globals.GetCluster(name.ClusterName);
            }
        }

        private static DatabaseSymbol GetDatabase(QualifiedName name, GlobalState globals)
        {
            var cluster = GetCluster(name, globals);
            if ((cluster == null || cluster == globals.Cluster) && string.IsNullOrEmpty(name.DatabaseName))
            {
                return globals.Database;
            }
            else
            {
                return cluster.GetDatabase(name.DatabaseName);
            }
        }

        private static TableSymbol GetTable(QualifiedName name, GlobalState globals)
        {
            var database = GetDatabase(name, globals);
            return database?.GetTable(name.EntityName);
        }

        private static ColumnSymbol GetColumn(QualifiedName name, GlobalState globals)
        {
            var table = GetTable(name, globals);
            return table?.GetColumn(name.ColumnName);
        }

        private static FunctionSymbol GetFunction(QualifiedName name, GlobalState globals)
        {
            var database = GetDatabase(name, globals);
            return database?.GetFunction(name.EntityName);
        }

        private class QualifiedName
        {
            public string ClusterName { get; }
            public string DatabaseName { get; }
            public string EntityName { get; }
            public string ColumnName { get; }

            public QualifiedName(string cluster, string database, string table, string column)
            {
                this.ClusterName = cluster ?? "";
                this.DatabaseName = database ?? "";
                this.EntityName = table ?? "";
                this.ColumnName = column ?? "";
            }

            public static QualifiedName ParseDatabase(string name)
            {
                var parts = name.Split(".");
                switch (parts.Length)
                {
                    case 2:
                        return new QualifiedName(parts[0], parts[1], null, null);
                    case 1:
                        return new QualifiedName(null, parts[0], null, null);
                    default:
                        throw new InvalidOperationException("Invalid database name");
                }
            }

            public static QualifiedName ParseEntity(string name)
            {
                var parts = name.Split(".");
                switch (parts.Length)
                {
                    case 3:
                        return new QualifiedName(parts[0], parts[1], parts[2], null);
                    case 2:
                        return new QualifiedName(null, parts[0], parts[1], null);
                    case 1:
                        return new QualifiedName(null, null, parts[0], null);
                    default:
                        throw new InvalidOperationException("Invalid table name");
                }
            }

            public static QualifiedName ParseColumn(string name)
            {
                var parts = name.Split(".");
                switch (parts.Length)
                {
                    case 4:
                        return new QualifiedName(parts[0], parts[1], parts[2], parts[3]);
                    case 3:
                        return new QualifiedName(null, parts[0], parts[1], parts[2]);
                    case 2:
                        return new QualifiedName(null, null, parts[0], parts[1]);
                    case 1:
                        return new QualifiedName(null, null, null, parts[0]);
                    default:
                        throw new InvalidOperationException("Invalid column name");
                }
            }
        }

        #endregion
    }
}