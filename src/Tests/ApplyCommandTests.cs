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
    public class ApplyCommandTests
    {
        private static readonly DatabaseSymbol _db = 
            new DatabaseSymbol("db");

        private static readonly ClusterSymbol _cluster =
            new ClusterSymbol("cluster", _db);

        private static readonly GlobalState _globals =
            GlobalState.Default
                .WithCluster(_cluster)
                .WithDatabase(_db);

        private void TestApply(GlobalState globals, string command, Action<GlobalState> checkResult = null, ApplyKind kind = ApplyKind.Strict)
        {
            var result = globals.ApplyCommand(command, kind);
            Assert.IsTrue(result.Succeeded);
            checkResult?.Invoke(result.Globals);
        }

        private void TestApply(GlobalState globals, IEnumerable<string> commands, Action<GlobalState> checkResult = null, ApplyKind kind = ApplyKind.Strict)
        {
            var result = globals.ApplyCommands(commands, kind);
            Assert.IsTrue(result.Succeeded);
            checkResult?.Invoke(result.Globals);
        }

        private void TestApplyFails(GlobalState globals, string command, Action<ApplyCommandResult> checkResult = null, ApplyKind kind = ApplyKind.Strict)
        {
            var result = globals.ApplyCommand(command, kind);
            Assert.IsFalse(result.Succeeded);
            checkResult?.Invoke(result);
        }

        #region Functions

        [TestMethod]
        public void TestCreateFunction()
        {
            // creating a function that does not exist adds the function
            TestApply(
                _globals,
                ".create function F(x: string) { T }",
                globals =>
                {
                    var fn = globals.Database.GetFunction("F");
                    Assert.IsNotNull(fn);
                });

            // creating a function that already exists fails
            TestApplyFails(
                _globals.AddOrUpdateDatabaseMembers(new FunctionSymbol("F", "(x: string)", "{ T }")),
                ".create function F() { T }"
                );

            // create with docstring
            TestApply(
                _globals,
                ".create function with (docstring = 'description') F() { T }",
                globals =>
                {
                    var f = globals.Database.GetFunction("F");
                    Assert.IsNotNull(f);
                    Assert.AreEqual("description", f.Description);
                });

            // adding a function that does not exists with ifnotexists, succeeds
            TestApply(
                _globals,
                ".create function ifnotexists F(x: string) { T }",
                globals =>
                {
                    var fn = globals.Database.GetFunction("F");
                    Assert.IsNotNull(fn);
                });

            // adding a function that already exists with ifnotexists also succeeds,
            // but does not overwrite the function
            TestApply(
                _globals.AddOrUpdateDatabaseMembers(new FunctionSymbol("F", "()", "{ T }")),
                ".create function ifnotexists F(x: string) { T }",
                globals =>
                {
                    var fn2 = globals.Database.GetFunction("F");
                    Assert.IsNotNull(fn2);
                    Assert.AreEqual(0, fn2.Signatures[0].Parameters.Count);
                });
        }

        [TestMethod]
        public void TestAlterFunction()
        {
            // altering a function that does not exist fails
            TestApplyFails(
                _globals,
                ".alter function F(x: string) { T }"
                );

            // altering a function that does exist succeeds
            TestApply(
                _globals.AddOrUpdateDatabaseMembers(new FunctionSymbol("F", "()", "{ T }")), 
                ".alter function F(x: string) { T }",
                globals =>
                {
                    var fn = globals.Database.GetFunction("F");
                    Assert.IsNotNull(fn);
                    Assert.AreEqual(1, fn.Signatures[0].Parameters.Count);
                });

            // altering function and change docstring
            TestApply(
                _globals.AddOrUpdateDatabaseMembers(new FunctionSymbol("F", "()", "{ T }")),
                ".alter function with (docstring = 'description') F() { T }",
                globals =>
                {
                    var f = globals.Database.GetFunction("F");
                    Assert.IsNotNull(f);
                    Assert.AreEqual("description", f.Description);
                });
        }

        [TestMethod]
        public void TestAlterFunctionDocString()
        {
            // alter function that exits docstring
            TestApply(
                _globals.AddOrUpdateDatabaseMembers(new FunctionSymbol("F", "()", "{ T }")), 
                ".alter function F docstring 'description'",
                globals =>
                {
                    var fn = globals.Database.GetFunction("F");
                    Assert.IsNotNull(fn);
                    Assert.AreEqual("description", fn.Description);
                });

            // alter function that that does not exist docstring
            TestApplyFails(
                _globals,
                ".alter function F docstring 'description'"
                );
        }

        [TestMethod]
        public void TestCreateOrAlterFunction()
        {
            // create-or-alter on a function that does not exist adds the function
            TestApply(
                _globals, 
                ".create-or-alter function F(x: string) { T }",
                globals =>
                {
                    var fn = globals.Database.GetFunction("F");
                    Assert.IsNotNull(fn);
                    Assert.AreEqual(1, fn.Signatures[0].Parameters.Count);
                });

            // create-or-alter a function that does exists alters the function
            TestApply(
                _globals.AddOrUpdateDatabaseMembers(new FunctionSymbol("F", "(x: string)", "{ T }")),
                ".create-or-alter function F() { T }",
                globals =>
                {
                    var fn2 = globals.Database.GetFunction("F");
                    Assert.IsNotNull(fn2);
                    Assert.AreEqual(0, fn2.Signatures[0].Parameters.Count);
                });

            // create-or-alter with docstring change
            TestApply(
                _globals,
                ".create-or-alter function with (docstring = 'description') F(x: string) { T }",
                globals =>
                {
                    var fn = globals.Database.GetFunction("F");
                    Assert.IsNotNull(fn);
                    Assert.AreEqual("description", fn.Description);
                });
        }

        [TestMethod]
        public void TestDropFunction()
        {
            // drop function that does not exist fails
            TestApplyFails(
                _globals,
                ".drop function F"
                );

            // drop function that does not exist with ifexists succeeds
            TestApply(
                _globals,
                ".drop function F ifexists"
                );

            // drop function that exists succeeds
            TestApply(
                _globals.AddOrUpdateDatabaseMembers(new FunctionSymbol("F", "()", "{ T }")),
                ".drop function F",
                globals =>
                {
                    Assert.IsNull(globals.Database.GetFunction("F"));
                });
        }

        [TestMethod]
        public void TestDropFunctions()
        {
            // drop functions that does not exist fails
            TestApplyFails(
                _globals,
                ".drop functions (F)"
                );

            // drop functions that does not exist with ifexists succeeds
            TestApply(
                _globals,
                ".drop functions (F) ifexists"
                );

            // drop functions that not all exist fails
            TestApplyFails(
                _globals.AddOrUpdateDatabaseMembers(new FunctionSymbol("F", "()", "{ T }")),
                ".drop functions (F, X)"
                );

            // drop functions that not all exist with ifexits succeeds
            TestApply(
                _globals.AddOrUpdateDatabaseMembers(new FunctionSymbol("F", "()", "{ T }")),
                ".drop functions (F, X) ifexists"
                );

            // drop functions that all exist succeeds
            TestApply(
                _globals.AddOrUpdateDatabaseMembers(
                    new FunctionSymbol("F1", "()", "{ T }"),
                    new FunctionSymbol("F2", "()", "{ T }")),
                ".drop functions (F1, F2)",
                globals =>
                {
                    Assert.IsNull(globals.Database.GetFunction("F1"));
                    Assert.IsNull(globals.Database.GetFunction("F2"));
                });
        }

        #endregion

        #region Tables
        [TestMethod]
        public void TestCreateTable()
        {
            // create new table succeeds
            TestApply(
                _globals, 
                ".create table T (x: long, y: string)",
                globals =>
                {
                    Assert.AreEqual(1, globals.Database.Tables.Count);
                });

            // create with existing table fails
            TestApplyFails(
                _globals.AddOrUpdateDatabaseMembers(new TableSymbol("T", "(x: long)")),
                ".create table T (x: long, y: string)"
                );

            // create table with docstring property sets description too
            TestApply(
                _globals,
                ".create table T (x: long, y: string) with (docstring = 'description')",
                globals =>
                {
                    var t = globals.Database.GetTable("T");
                    Assert.IsNotNull(t);
                    Assert.AreEqual("description", t.Description);
                });
        }

        [TestMethod]
        public void TestCreateTables()
        {
            // create new tables succeeds
            TestApply(
                _globals, 
                ".create tables T1(x: long, y: string), T2(p: long, q: string)",
                globals =>
                {
                    Assert.AreEqual(2, globals.Database.Tables.Count);
                });

            // create any existing table fails
            TestApplyFails(
                _globals.AddOrUpdateDatabaseMembers(new TableSymbol("T2", "(x: long)")),
                ".create tables T1(x: long, y: string), T2(p: long, q: string)"
                );

            // create tables with docstring
            TestApply(
                _globals,
                ".create tables T1(x: long, y: string), T2(p: long, q: string) with (docstring = 'description')",
                globals =>
                {
                    Assert.AreEqual(2, globals.Database.Tables.Count);
                    Assert.IsTrue(globals.Database.Tables.All(t => t.Description == "description"));
                });
        }

        [TestMethod]
        public void TestCreateTableBasedOn()
        {
            // create new table from existing table succeeds
            TestApply(
                _globals.AddOrUpdateDatabaseMembers(new TableSymbol("T1", "(x: long)")),
                ".create table T2 based-on T1",
                globals =>
                {
                    Assert.AreEqual(2, globals.Database.Tables.Count);
                });

            // create new table from unknown table fails
            TestApplyFails(
                _globals,
                ".create table T2 based-on T1"
                );

            // create existing table from existing table fails
            TestApplyFails(
                _globals.AddOrUpdateDatabaseMembers(
                    new TableSymbol("T1", "(x: long)"),
                    new TableSymbol("T2", "(y: string)")),
                ".create table T2 based-on T1"
                );

            // create existing table from existing table with ifnotexists defined succeeds
            TestApply(
                _globals.AddOrUpdateDatabaseMembers(
                    new TableSymbol("T1", "(x: long)"),
                    new TableSymbol("T2", "(y: string)")),
                ".create table T2 based-on T1 ifnotexists",
                globals =>
                {
                    Assert.AreEqual(2, globals.Database.Tables.Count);
                });
        }

        [TestMethod]
        public void TestCreateMergeTable()
        {
            // create-merge with new table succeeds
            TestApply(
                _globals, 
                ".create-merge table T (x: long, y: string)",
                globals =>
                {
                    Assert.AreEqual(1, globals.Database.Tables.Count);
                    Assert.AreEqual(2, globals.Database.Tables[0].Columns.Count);
                });

            // create-merge with existing table succeeds (merges schema)
            TestApply(
                _globals.AddOrUpdateDatabaseMembers(new TableSymbol("T", "(a: long, b: long)")),
                ".create-merge table T (b: long, c: long)",
                globals =>
                {
                    Assert.AreEqual(1, globals.Database.Tables.Count);
                    Assert.AreEqual(3, globals.Database.Tables[0].Columns.Count);
                });

            // create-merge with conflicting column definitions fails.
            TestApplyFails(
                _globals.AddOrUpdateDatabaseMembers(new TableSymbol("T", "(a: long, b: long)")),
                ".create-merge table T (b: string, c: long)"
                );

            // create-merge with docstring property sets description
            TestApply(
                _globals,
                ".create-merge table T (x: long, y: string) with (docstring = 'description')",
                globals =>
                {
                    var t = globals.Database.GetTable("T");
                    Assert.IsNotNull(t);
                    Assert.AreEqual("description", t.Description);
                });
        }

        [TestMethod]
        public void TestCreateMergeTables()
        {
            // create merge new tables succeeds
            TestApply(
                _globals,
                ".create-merge tables T1(x: long, y: string), T2(p: long, q: string)",
                globals =>
                {
                    Assert.AreEqual(2, globals.Database.Tables.Count);
                });

            // create merge existing tables succeeds (merges schema)
            TestApply(
                _globals.AddOrUpdateDatabaseMembers(new TableSymbol("T1", "(x: long, z: real)")),
                ".create-merge tables T1(x: long, y: string), T2(p: long, q: string)",
                globals =>
                {
                    Assert.AreEqual(2, globals.Database.Tables.Count);
                    var t1 = globals.Database.GetTable("T1");
                    Assert.IsNotNull(t1);
                    Assert.AreEqual(3, t1.Columns.Count);
                    var t2 = globals.Database.GetTable("T2");
                    Assert.IsNotNull(t2);
                    Assert.AreEqual(2, t2.Columns.Count);
                });
        }

        [TestMethod]
        public void TestAlterTable()
        {
            // alter table that exists succeeds
            TestApply(
                _globals.AddOrUpdateDatabaseMembers(new TableSymbol("T", "(x: long, y: string)")),
                ".alter table T (x: long, z: real)",
                globals =>
                {
                    var t = globals.Database.GetTable("T");
                    Assert.IsNotNull(t);
                    Assert.AreEqual(2, t.Columns.Count);
                    var z = t.GetColumn("z");
                    Assert.IsNotNull(z);
                });

            // alter table that does not exist fails
            TestApplyFails(
                _globals,
                ".alter table T (x: long, z: real)"
                );

            // alter table that exits with docstring property sets description
            TestApply(
                _globals.AddOrUpdateDatabaseMembers(new TableSymbol("T", "(x: long, y: string)")),
                ".alter table T (x: long, y: string) with (docstring = 'description')",
                globals =>
                {
                    var t = globals.Database.GetTable("T");
                    Assert.IsNotNull(t);
                    Assert.AreEqual("description", t.Description);
                });
        }

        [TestMethod]
        public void TestAlterTableDocString()
        {
            // altering docstring of table that exists succeeds
            TestApply(
                _globals.AddOrUpdateDatabaseMembers(new TableSymbol("T", "(x: long, y: string)")),
                ".alter table T docstring 'description'",
                globals =>
                {
                    var t = globals.Database.GetTable("T");
                    Assert.IsNotNull(t);
                    Assert.AreEqual("description", t.Description);
                });

            // altering docstring of table that does not exist fails
            TestApplyFails(
                _globals,
                ".alter table T docstring 'description'"
                );
        }

        [TestMethod]
        public void TestAlterMergeTable()
        {
            // alter-merge existing table succeeds
            TestApply(
                _globals.AddOrUpdateDatabaseMembers(new TableSymbol("T", "(x: long, y: string)")),
                ".alter-merge table T (x: long, z: real)",
                globals =>
                {
                    var t = globals.Database.GetTable("T");
                    Assert.IsNotNull(t);
                    Assert.AreEqual(3, t.Columns.Count);
                });

            // alter-merge non-existing table fails
            TestApplyFails(
                _globals,
                ".alter-merge table T (x: long, z: real)"
                );

            // alter-merge existing table with docstring property
            TestApply(
                _globals.AddOrUpdateDatabaseMembers(new TableSymbol("T", "(x: long, y: string)")),
                ".alter-merge table T (x: long, z: real) with (docstring = 'description')",
                globals =>
                {
                    var t = globals.Database.GetTable("T");
                    Assert.IsNotNull(t);
                    Assert.AreEqual("description", t.Description);
                });
        }

        [TestMethod]
        public void TestRenameTable()
        {
            // rename table that exists succeeds
            TestApply(
                _globals.AddOrUpdateDatabaseMembers(new TableSymbol("T", "(x: long, y: string)")),
                ".rename table T to Tee",
                globals =>
                {
                    Assert.AreEqual(1, globals.Database.Tables.Count);
                    var tee = globals.Database.GetTable("Tee");
                    Assert.IsNotNull(tee);
                });

            // rename table that does not exist fails
            TestApplyFails(
                _globals,
                ".rename table T to Tee"
                );

            // rename table to name that already exists fails
            TestApplyFails(
                _globals.AddOrUpdateDatabaseMembers(
                    new TableSymbol("T", "(x: long)"),
                    new TableSymbol("Tee", "(y: string")),
                ".rename table T to Tee"
                );
        }

        [TestMethod]
        public void TestRenameTables()
        {
            // rename tables that exist succeeds
            TestApply(
                _globals.AddOrUpdateDatabaseMembers(
                    new TableSymbol("T1", "(x: long, y: string)"),
                    new TableSymbol("T2", "(a: long, b: real, c: string)")),
                ".rename tables TeeOne=T1, TeeTwo=T2",
                globals =>
                {
                    Assert.AreEqual(2, globals.Database.Tables.Count);
                    var teeOne = globals.Database.GetTable("TeeOne");
                    Assert.IsNotNull(teeOne);
                    var teeTwo = globals.Database.GetTable("TeeTwo");
                    Assert.IsNotNull(teeTwo);
                });

            // rename tables with any table non-existing fails
            TestApplyFails(
                _globals.AddOrUpdateDatabaseMembers(
                    new TableSymbol("T1", "(x: long, y: string)")),
                ".rename tables TeeOne=T1, TeeTwo=T2"
                );

            // rename tables with any new name already existing fails
            TestApplyFails(
                _globals.AddOrUpdateDatabaseMembers(
                    new TableSymbol("T1", "(x: long, y: string)"),
                    new TableSymbol("T2", "(a: long, b: real, c: string)"),
                    new TableSymbol("TeeTwo", "(abc: string)")
                    ),
                ".rename tables TeeOne=T1, TeeTwo=T2"
                );
        }

        [TestMethod]
        public void TestDropTable()
        {
            // drop existing table succeeds
            TestApply(
                _globals.AddOrUpdateDatabaseMembers(new TableSymbol("T", "(x: long, y: string)")),
                ".drop table T",
                globals =>
                {
                    Assert.AreEqual(0, globals.Database.Tables.Count);
                });

            // drop non-existing table fails
            TestApplyFails(
                _globals,
                ".drop table T"
                );

            // drop non-existing table with ifexists succeeds
            TestApply(
                _globals,
                ".drop table T ifexists"
                );
        }

        [TestMethod]
        public void TestDropTables()
        {
            // drop existing tables succeeds
            TestApply(
                _globals.AddOrUpdateDatabaseMembers(
                    new TableSymbol("T1", "(x: long, y: string)"),
                    new TableSymbol("T2", "(a: long, b: real, c: string)")),
                ".drop tables (T1, T2)",
                globals =>
                {
                    Assert.AreEqual(0, globals.Database.Tables.Count);
                });

            // drop any non-existing table fails
            TestApplyFails(
                _globals.AddOrUpdateDatabaseMembers(
                    new TableSymbol("T1", "(x: long, y: string)")),
                ".drop tables (T1, T2)"
                );

            // drop any non-existing table with ifexists succeeds
            TestApply(
                _globals.AddOrUpdateDatabaseMembers(
                    new TableSymbol("T1", "(x: long, y: string)")),
                ".drop tables (T1, T2) ifexists"
                );
        }

        [TestMethod]
        public void TestSetTable()
        {
            // set new table from input succeeds
            TestApply(
                _globals.AddOrUpdateDatabaseMembers(new TableSymbol("T", "(x: long, y: string)")),
                ".set T2 <| T",
                globals =>
                {
                    var t2 = globals.Database.GetTable("T2");
                    Assert.IsNotNull(t2);
                    Assert.AreEqual("(x: long, y: string)", t2.ToTestString());
                });

            // set new table from invalid input fails
            TestApplyFails(
                _globals,
                ".set T2 <| T"
                );

            // set existing table from input fails
            TestApplyFails(
                _globals.AddOrUpdateDatabaseMembers(
                    new TableSymbol("T", "(x: long)"),
                    new TableSymbol("T2", "(y: string)")),
                ".set T2 <| T"
                );

            // set new table with docstring property
            TestApply(
                _globals.AddOrUpdateDatabaseMembers(new TableSymbol("T", "(x: long, y: string)")),
                ".set T2 with (docstring='description') <| T",
                globals =>
                {
                    var t2 = globals.Database.GetTable("T2");
                    Assert.IsNotNull(t2);
                    Assert.AreEqual("description", t2.Description);
                });
        }

        [TestMethod]
        public void TestAppendTable()
        {
            // append existing table from input without extend_schema
            TestApply(
                _globals.AddOrUpdateDatabaseMembers(new TableSymbol("T", "(x: long, y: string)")),
                ".append T <| T | extend z=1.0",
                globals =>
                {
                    var t = globals.Database.GetTable("T");
                    Assert.IsNotNull(t);
                    Assert.AreEqual("(x: long, y: string)", t.ToTestString());
                });

            // append existing table from input with extend_schema
            TestApply(
                _globals.AddOrUpdateDatabaseMembers(new TableSymbol("T", "(x: long, y: string)")),
                ".append T with (extend_schema=true) <| T | extend z=1.0",
                globals =>
                {
                    var t = globals.Database.GetTable("T");
                    Assert.IsNotNull(t);
                    Assert.AreEqual("(x: long, y: string, z: real)", t.ToTestString());
                });

            // append new table from input fails
            TestApplyFails(
                _globals,
                ".append T <| print z=1.0"
                );

            // append with docstring property
            TestApply(
                _globals.AddOrUpdateDatabaseMembers(new TableSymbol("T", "(x: long, y: string)")),
                ".append T with (docstring='description') <| T",
                globals =>
                {
                    var t = globals.Database.GetTable("T");
                    Assert.IsNotNull(t);
                    Assert.AreEqual("description", t.Description);
                });
        }

        [TestMethod]
        public void TestSetOrAppendTable()
        {
            // set-or-append non-existing table from input succeeds
            TestApply(
                _globals.AddOrUpdateDatabaseMembers(new TableSymbol("T", "(a: long, b: string, c: real)")),
                ".set-or-append T2 <| T",
                globals =>
                {
                    var t2 = globals.Database.GetTable("T2");
                    Assert.IsNotNull(t2);
                    Assert.AreEqual("(a: long, b: string, c: real)", t2.ToTestString());
                });

            // set-or-append existing table from input succeeds (without extend_schema)
            TestApply(
                _globals.AddOrUpdateDatabaseMembers(
                    new TableSymbol("T", "(a: long, b: string, c: real)"),
                    new TableSymbol("T2", "(x: long, y: string)")),
                ".set-or-append T2 <| T",
                globals =>
                {
                    var t2 = globals.Database.GetTable("T2");
                    Assert.IsNotNull(t2);
                    Assert.AreEqual("(x: long, y: string)", t2.ToTestString());
                });

            // set-or-append existing table form input (with extend_schema)
            TestApply(
                _globals.AddOrUpdateDatabaseMembers(
                    new TableSymbol("T", "(a: long, b: string, c: real)"),
                    new TableSymbol("T2", "(x: long, y: string)")),
                ".set-or-append T2 with (extend_schema=true) <| T",
                globals =>
                {
                    var t2 = globals.Database.GetTable("T2");
                    Assert.IsNotNull(t2);
                    Assert.AreEqual("(x: long, y: string, c: real)", t2.ToTestString());
                });

            // set-or-append with docstring property
            TestApply(
                _globals.AddOrUpdateDatabaseMembers(new TableSymbol("T", "(a: long, b: string)")),
                ".set-or-append T2 with (docstring='description') <| T",
                globals =>
                {
                    var t2 = globals.Database.GetTable("T2");
                    Assert.IsNotNull(t2);
                    Assert.AreEqual("description", t2.Description);
                });
        }

        [TestMethod]
        public void TestSetOrReplaceTable()
        {
            // set-or-replace non-existing table from input
            TestApply(
                _globals.AddOrUpdateDatabaseMembers(new TableSymbol("T", "(a: long, b: string)")),
                ".set-or-replace T2 <| T",
                globals =>
                {
                    var t2 = globals.Database.GetTable("T2");
                    Assert.IsNotNull(t2);
                    Assert.AreEqual("(a: long, b: string)", t2.ToTestString());
                });

            // set-or-replace existing table from input (without extend_schema)
            TestApply(
                _globals.AddOrUpdateDatabaseMembers(
                    new TableSymbol("T", "(a: long, b: string, c: real)"),
                    new TableSymbol("T2", "(x: long, y: string)")),
                ".set-or-replace T2 <| T",
                globals =>
                {
                    var t2 = globals.Database.GetTable("T2");
                    Assert.IsNotNull(t2);
                    Assert.AreEqual("(x: long, y: string)", t2.ToTestString());
                });

            // set-or-replace existing table from input (with extend_schema)
            TestApply(
                _globals.AddOrUpdateDatabaseMembers(
                    new TableSymbol("T", "(a: long, b: string, c: real)"),
                    new TableSymbol("T2", "(x: long, y: string)")),
                ".set-or-replace T2 with (extend_schema=true) <| T",
                globals =>
                {
                    var t2 = globals.Database.GetTable("T2");
                    Assert.IsNotNull(t2);
                    Assert.AreEqual("(x: long, y: string, c: real)", t2.ToTestString());
                });

            // set-or-replace existing table (with recreate_schema)
            TestApply(
                _globals.AddOrUpdateDatabaseMembers(
                    new TableSymbol("T", "(a: long, b: string, c: real)"),
                    new TableSymbol("T2", "(x: long, y: string)")),
                ".set-or-replace T2 with (recreate_schema=true) <| T",
                globals =>
                {
                    var t2 = globals.Database.GetTable("T2");
                    Assert.IsNotNull(t2);
                    Assert.AreEqual("(a: long, b: string, c: real)", t2.ToTestString());
                });

            // set-or-replace with docstring property
            TestApply(
                _globals.AddOrUpdateDatabaseMembers(new TableSymbol("T", "(x: long, y: string)")),
                ".set-or-replace T2 with (docstring='description') <| T",
                globals =>
                {
                    var t2 = globals.Database.GetTable("T2");
                    Assert.IsNotNull(t2);
                    Assert.AreEqual("description", t2.Description);
                });
        }

        #endregion

        #region Columns

        [TestMethod]
        public void TestAlterColumnType()
        {
            // alter column type with <table>.<column>
            TestApply(
                _globals.AddOrUpdateDatabaseMembers(new TableSymbol("T", "(x: long, y: string)")),
                ".alter column T.y type = real",
                globals =>
                {
                    var t = globals.Database.GetTable("T");
                    Assert.IsNotNull(t);
                    Assert.AreEqual("(x: long, y: real)", t.ToTestString());
                });

            // alter column type with <db>.<table>.<column>
            TestApply(
                _globals.AddOrUpdateClusterDatabase(
                    new DatabaseSymbol("db2", new TableSymbol("T", "(x: long, y: string)"))),
                ".alter column ['db2'].T.y type = real",
                globals =>
                {
                    var t = globals.Cluster.GetDatabase("db2").GetTable("T");
                    Assert.IsNotNull(t);
                    Assert.AreEqual("(x: long, y: real)", t.ToTestString());
                });

            // alter column type of unknown table fails
            TestApplyFails(
                _globals,
                ".alter column T.y type = real"
                );

            // alter column type of unknown column fails
            TestApplyFails(
                _globals.AddOrUpdateDatabaseMembers(new TableSymbol("T", "(x: long, y: string)")),
                ".alter column T.z type = real"
                );

            // alter column type of unknown database fails
            TestApplyFails(
                _globals.AddOrUpdateDatabaseMembers(new TableSymbol("T", "(x: long, y: string)")),
                ".alter column DB.T.x type = real"
                );
        }

        [TestMethod]
        public void TestDropColumn()
        {
            // drop column <table>.<column>
            TestApply(
                _globals.AddOrUpdateDatabaseMembers(new TableSymbol("T", "(x: long, y: string)")),
                ".drop column T.y",
                globals =>
                {
                    var t = globals.Database.GetTable("T");
                    Assert.IsNotNull(t);
                    Assert.AreEqual("(x: long)", t.ToTestString());
                });

            // drop column <database>.<table>.<column>
            TestApply(
                _globals.AddOrUpdateClusterDatabase(
                    new DatabaseSymbol("db2", new TableSymbol("T", "(x: long, y: string)"))),
                ".drop column ['db2'].T.y",
                globals =>
                {
                    var t = globals.Cluster.GetDatabase("db2").GetTable("T");
                    Assert.IsNotNull(t);
                    Assert.AreEqual("(x: long)", t.ToTestString());
                });

            // drop non-existing column of existing table fails
            TestApplyFails(
                _globals.AddOrUpdateDatabaseMembers(new TableSymbol("T", "(x: long, y: string)")),
                ".drop column T.z"
                );

            // drop column of non-existing table fails
            TestApplyFails(
                _globals,
                ".drop column T.y"
                );

            // drop column of table of non-existing database fails
            TestApplyFails(
                _globals,
                ".drop column ['db2'].T.y"
                );
        }

        [TestMethod]
        public void TestDropTableColumns()
        {
            // drop columns of table
            TestApply(
                _globals.AddOrUpdateDatabaseMembers(new TableSymbol("T", "(x: long, y: string, z: real)")),
                ".drop table T columns (x, z)",
                globals =>
                {
                    var t = globals.Database.GetTable("T");
                    Assert.IsNotNull(t);
                    Assert.AreEqual("(y: string)", t.ToTestString());
                });

            // drop columns of unknown table fails
            TestApplyFails(
                _globals,
                ".drop table T columns (x, z)"
                );

            // drop any unknown column fails
            TestApplyFails(
                _globals.AddOrUpdateDatabaseMembers(new TableSymbol("T", "(x: long, y: string, z: real)")),
                ".drop table T columns (x, z, c)"
                );
        }

        [TestMethod]
        public void TestRenameColumn()
        {
            // rename column <table>.<column> to new name 
            TestApply(
                _globals.AddOrUpdateDatabaseMembers(new TableSymbol("T", "(x: long, y: string)")),
                ".rename column T.y to why",
                globals =>
                {
                    var t = globals.Database.GetTable("T");
                    Assert.IsNotNull(t);
                    Assert.AreEqual("(x: long, why: string)", t.ToTestString());
                });

            // rename column <database>.<table>.<column> to new name 
            TestApply(
                _globals.AddOrUpdateClusterDatabase(
                    new DatabaseSymbol("db2", new TableSymbol("T", "(x: long, y: string)"))),
                ".rename column ['db2'].T.y to why",
                globals =>
                {
                    var t = globals.Cluster.GetDatabase("db2").GetTable("T");
                    Assert.IsNotNull(t);
                    Assert.AreEqual("(x: long, why: string)", t.ToTestString());
                });

            // rename column <table>.<column> to existing name fails
            TestApplyFails(
                _globals.AddOrUpdateDatabaseMembers(new TableSymbol("T", "(x: long, y: string)")),
                ".rename column T.y to x"
                );

            // rename column from unknown table fails
            TestApplyFails(
                _globals,
                ".rename column T.y to x"
                );

            // rename column from table in unknown database fails
            TestApplyFails(
                _globals,
                ".rename column ['db2'].T.y to x"
                );
        }

        [TestMethod]
        public void TestRenameColumns()
        {
            // rename columns across multiple tables and databases
            TestApply(
                _globals
                    .AddOrUpdateClusterDatabases(
                        new DatabaseSymbol("db2",
                            new TableSymbol("T2", "(x: long, y: string)")))
                    .AddOrUpdateDatabaseMembers(
                        new TableSymbol("T", "(x: long, y: string)")),
                ".rename columns why=T.y, why2=db2.T2.y",
                globals =>
                {
                    var t = globals.Database.GetTable("T");
                    Assert.IsNotNull(t);
                    Assert.AreEqual("(x: long, why: string)", t.ToTestString());
                    var db2 = globals.Cluster.GetDatabase("db2");
                    Assert.IsNotNull(db2);
                    var t2 = db2.GetTable("T2");
                    Assert.IsNotNull(t2);
                    Assert.AreEqual("(x: long, why2: string)", t2.ToTestString());
                });

            // rename column from any unknown table fails
            TestApplyFails(
                _globals.AddOrUpdateDatabaseMembers(
                    new TableSymbol("T", "(x: long, y: string)")),
                ".rename columns why=T.y, why2=T2.y"
                );

            // rename column from any unknown database fails
            TestApplyFails(
                _globals.AddOrUpdateDatabaseMembers(
                    new TableSymbol("T", "(x: long, y: string)")),
                ".rename columns why=T.y, why2=['db2'].T2.y"
                );
        }

        [TestMethod]
        public void TestAlterTableColumnDocStrings()
        {
            // alter docstrings from existing table
            TestApply(
                _globals.AddOrUpdateDatabaseMembers(
                    new TableSymbol("T", "(x: long, y: string)")
                        .AddOrUpdateColumns(new ColumnSymbol("z", ScalarTypes.Real, "z description"))),
                ".alter table T column-docstrings (x: 'x description', y: 'y description')",
                globals =>
                {
                    var t = globals.Database.GetTable("T");
                    Assert.IsNotNull(t);
                    Assert.AreEqual(3, t.Columns.Count);
                    Assert.AreEqual("x description", t.Columns[0].Description);
                    Assert.AreEqual("y description", t.Columns[1].Description);
                    Assert.AreEqual("", t.Columns[2].Description); // unspecified column is cleared
                });

            // alter docstrings from non-existing table fails
            TestApplyFails(
                _globals,
                ".alter table T column-docstrings (x: 'x description', y: 'y description')"
                );
        }

        [TestMethod]
        public void TestAlterMergeTableColumnDocStrings()
        {
            // alter-merge docstrings from existing table
            TestApply(
                _globals.AddOrUpdateDatabaseMembers(
                    new TableSymbol("T", "(x: long, y: string)")
                        .AddOrUpdateColumns(new ColumnSymbol("z", ScalarTypes.Real, "z description"))),
                ".alter-merge table T column-docstrings (x: 'x description', y: 'y description')",
                globals =>
                {
                    var t = globals.Database.GetTable("T");
                    Assert.IsNotNull(t);
                    Assert.AreEqual(3, t.Columns.Count);
                    Assert.AreEqual("x description", t.Columns[0].Description);
                    Assert.AreEqual("y description", t.Columns[1].Description);
                    Assert.AreEqual("z description", t.Columns[2].Description); // unspecified column unchanged
                });

            // alter docstrings from non-existing table fails
            TestApplyFails(
                _globals,
                ".alter-merge table T column-docstrings (x: 'x description', y: 'y description')"
                );
        }

        #endregion

        #region External Tables

        [TestMethod]
        public void TestCreateExternalTable()
        {
            // create external table
            TestApply(
                _globals,
                ".create external table ET (x: long, y: string) kind=storage dataformat=json ('connection')",
                globals =>
                {
                    var et = globals.Database.GetExternalTable("ET");
                    Assert.IsNotNull(et);
                    Assert.AreEqual("(x: long, y: string)", et.ToTestString());
                });

            // create external table that already exists fails
            TestApplyFails(
                _globals.AddOrUpdateDatabaseMembers(new ExternalTableSymbol("ET", "(a: long, b: string)")),
                ".create external table ET (x: long, y: string) kind=storage dataformat=json ('connection')"
                );

            // create external table with docstring property
            TestApply(
                _globals,
                ".create external table ET (x: long, y: string) kind=storage dataformat=json ('connection') with (docstring='description')",
                globals =>
                {
                    var et = globals.Database.GetExternalTable("ET");
                    Assert.IsNotNull(et);
                    Assert.AreEqual("description", et.Description);
                });

            // create external table kind=sql
            TestApply(
                _globals,
                ".create external table ET (x: long, y: string) kind=sql table=Customers ('connection')",
                globals =>
                {
                    var et = globals.Database.GetExternalTable("ET");
                    Assert.IsNotNull(et);
                    Assert.AreEqual("(x: long, y: string)", et.ToTestString());
                });
        }

        [TestMethod]
        public void TestAlterExternalTable()
        {
            // alter existing external table
            TestApply(
                _globals.AddOrUpdateDatabaseMembers(new ExternalTableSymbol("ET", "(a: long, b: string)")),
                ".alter external table ET (x: long, y: string) kind=storage dataformat=json ('connection')",
                globals =>
                {
                    var et = globals.Database.GetExternalTable("ET");
                    Assert.IsNotNull(et);
                    Assert.AreEqual("(x: long, y: string)", et.ToTestString());
                });

            // alter non-existing external table fails
            TestApplyFails(
                _globals,
                ".alter external table ET (x: long, y: string) kind=storage dataformat=json ('connection')"
                );

            // alter external table with docstring property
            TestApply(
                _globals.AddOrUpdateDatabaseMembers(new ExternalTableSymbol("ET", "(a: long, b: string)")),
                ".alter external table ET (x: long, y: string) kind=storage dataformat=json ('connection') with (docstring='description')",
                globals =>
                {
                    var et = globals.Database.GetExternalTable("ET");
                    Assert.IsNotNull(et);
                    Assert.AreEqual("description", et.Description);
                });

            // alter external table kind=sql
            TestApply(
                _globals.AddOrUpdateDatabaseMembers(new ExternalTableSymbol("ET", "(a: long, b: string)")),
                ".alter external table ET (x: long, y: string) kind=sql table=Customers ('connection')",
                globals =>
                {
                    var et = globals.Database.GetExternalTable("ET");
                    Assert.IsNotNull(et);
                    Assert.AreEqual("(x: long, y: string)", et.ToTestString());
                });
        }

        [TestMethod]
        public void TestCreateOrAlterExternalTable()
        {
            // create-or-alter external storage that already exists
            TestApply(
                _globals.AddOrUpdateDatabaseMembers(new ExternalTableSymbol("ET", "(a: long, b: string)")),
                ".create-or-alter external table ET (x: long, y: string) kind=storage dataformat=json ('connection')",
                globals =>
                {
                    var et = globals.Database.GetExternalTable("ET");
                    Assert.IsNotNull(et);
                    Assert.AreEqual("(x: long, y: string)", et.ToTestString());
                });

            // create-or-alter external storage that does not exist
            TestApply(
                _globals,
                ".create-or-alter external table ET (x: long, y: string) kind=storage dataformat=json ('connection')",
                globals =>
                {
                    var et = globals.Database.GetExternalTable("ET");
                    Assert.IsNotNull(et);
                    Assert.AreEqual("(x: long, y: string)", et.ToTestString());
                });

            // create-or-alter with docstring property
            TestApply(
                _globals,
                ".create-or-alter external table ET (x: long, y: string) kind=storage dataformat=json ('connection') with (docstring='description')",
                globals =>
                {
                    var et = globals.Database.GetExternalTable("ET");
                    Assert.IsNotNull(et);
                    Assert.AreEqual("description", et.Description);
                });

            // create-or-alter external storage kind=sql
            TestApply(
                _globals.AddOrUpdateDatabaseMembers(new ExternalTableSymbol("ET", "(a: long, b: string)")),
                ".create-or-alter external table ET (x: long, y: string) kind=sql table=Customers ('connection')",
                globals =>
                {
                    var et = globals.Database.GetExternalTable("ET");
                    Assert.IsNotNull(et);
                    Assert.AreEqual("(x: long, y: string)", et.ToTestString());
                });
        }

        [TestMethod]
        public void TestDropExternalTable()
        {
            // drop existing external table
            TestApply(
                _globals.AddOrUpdateDatabaseMembers(new ExternalTableSymbol("ET", "(a: long, b: string)")),
                ".drop external table ET",
                globals =>
                {
                    var et = globals.Database.GetExternalTable("ET");
                    Assert.IsNull(et);
                });

            // drop non-existing external table fails
            TestApplyFails(
                _globals,
                ".drop external table ET"
                );
        }

        #endregion

        #region Materialized Views

        [TestMethod]
        public void TestCreateMaterializedView()
        {
            // create new materialized-view
            TestApply(
                _globals.AddOrUpdateDatabaseMembers(new TableSymbol("T", "(x: long, y: string)")),
                ".create materialized-view V on table T { T | extend z=1.0 }",
                globals =>
                {
                    var v = globals.Database.GetMaterializedView("V");
                    Assert.IsNotNull(v);
                    Assert.AreEqual("(x: long, y: string, z: real)", v.ToTestString());
                });

            // create materialized-view that already exists fails
            TestApplyFails(
                _globals.AddOrUpdateDatabaseMembers(
                    new TableSymbol("T", "(x: long, y: string)"),
                    new MaterializedViewSymbol("V", "(x: long, y: string)", "")),
                ".create materialized-view V on table T { T | extend z=1.0 }");

            // create materialized-view that already exists with ifnotexists succeeds
            TestApply(
                _globals.AddOrUpdateDatabaseMembers(
                    new TableSymbol("T", "(x: long, y: string)"),
                    new MaterializedViewSymbol("V", "(x: long, y: string)", "")),
                ".create ifnotexists materialized-view V on table T { T | extend z=1.0 }",
                globals =>
                {
                    var v = globals.Database.GetMaterializedView("V");
                    Assert.IsNotNull(v);
                    Assert.AreEqual("(x: long, y: string)", v.ToTestString());
                });

            // create materialied-view with docstring property
            TestApply(
                _globals.AddOrUpdateDatabaseMembers(new TableSymbol("T", "(x: long, y: string)")),
                ".create materialized-view with (docstring='description') V on table T { T | extend z=1.0 }",
                globals =>
                {
                    var v = globals.Database.GetMaterializedView("V");
                    Assert.IsNotNull(v);
                    Assert.AreEqual("description", v.Description);
                });

            // create new materialized-view over table that does not exist
            TestApplyFails(
                _globals,
                ".create materialized-view V on table T { T | extend z=1.0 }"
                );

            // create new materialized-view over materialized-view
            TestApply(
                _globals.AddOrUpdateDatabaseMembers(new MaterializedViewSymbol("V", "(x: long, y: string)", "")),
                ".create materialized-view V2 on materialized-view V { V | extend z=1.0 }",
                globals =>
                {
                    var v = globals.Database.GetMaterializedView("V2");
                    Assert.IsNotNull(v);
                    Assert.AreEqual("(x: long, y: string, z: real)", v.ToTestString());
                });

            // create new materialized-view over materialized-view that does not exist
            TestApplyFails(
                _globals,
                ".create materialized-view V2 on materialized-view V { V | extend z=1.0 }"
                );
        }

        [TestMethod]
        public void TestAlterMaterializedView()
        {
            // alter existing materialized-view
            TestApply(
                _globals.AddOrUpdateDatabaseMembers(
                    new TableSymbol("T", "(x: long, y: string)"),
                    new MaterializedViewSymbol("V", "(a: long, b: long)", "")),
                ".alter materialized-view V on table T { T | extend z=1.0 }",
                globals =>
                {
                    var v = globals.Database.GetMaterializedView("V");
                    Assert.IsNotNull(v);
                    Assert.AreEqual("(x: long, y: string, z: real)", v.ToTestString());
                });

            // alter materialized-view that does not exist fails
            TestApplyFails(
                _globals.AddOrUpdateDatabaseMembers(new TableSymbol("T", "(x: long, y: string)")),
                ".alter materialized-view V on table T { T | extend z=1.0 }"
                );

            // alter materialized-view with docstring property
            TestApply(
                _globals.AddOrUpdateDatabaseMembers(
                    new TableSymbol("T", "(x: long, y: string)"),
                    new MaterializedViewSymbol("V", "(a: long, b: long)", "")),
                ".alter materialized-view with (docstring='description') V on table T { T | extend z=1.0 }",
                globals =>
                {
                    var v = globals.Database.GetMaterializedView("V");
                    Assert.IsNotNull(v);
                    Assert.AreEqual("description", v.Description);
                });

            // alter materialized-view on materialized view
            TestApply(
                _globals.AddOrUpdateDatabaseMembers(
                    new MaterializedViewSymbol("V", "(a: long, a: long)", ""),
                    new MaterializedViewSymbol("V2", "(a: long, b: long)", "")),
                ".alter materialized-view V on materialized-view V2 { V2 | extend z=1.0 }",
                globals =>
                {
                    var v = globals.Database.GetMaterializedView("V");
                    Assert.IsNotNull(v);
                    Assert.AreEqual("(a: long, b: long, z: real)", v.ToTestString());
                });
        }

        [TestMethod]
        public void TestAlterMaterializedViewDocString()
        {
            // alter materialized-view with docstring property
            TestApply(
                _globals.AddOrUpdateDatabaseMembers(
                    new TableSymbol("T", "(x: long, y: string)"),
                    new MaterializedViewSymbol("V", "(a: long, b: long)", "")),
                ".alter materialized-view V docstring 'description'",
                globals =>
                {
                    var v = globals.Database.GetMaterializedView("V");
                    Assert.IsNotNull(v);
                    Assert.AreEqual("description", v.Description);
                });

            // alter non-existing materialized-view with docstring property 
            TestApplyFails(
                _globals,
                ".alter materialized-view V docstring 'description'"
                );
        }

        [TestMethod]
        public void TestCreateOrAlterMaterializedView()
        {
            // create-or-alter non-existing materialized view creates it
            TestApply(
                _globals.AddOrUpdateDatabaseMembers(new TableSymbol("T", "(x: long, y: string)")),
                ".create-or-alter materialized-view V on table T { T | extend z=1.0 }",
                globals =>
                {
                    var v = globals.Database.GetMaterializedView("V");
                    Assert.IsNotNull(v);
                    Assert.AreEqual("(x: long, y: string, z: real)", v.ToTestString());
                });

            // create-or-alter existing materialized-view alters it
            TestApply(
                _globals.AddOrUpdateDatabaseMembers(
                    new TableSymbol("T", "(x: long, y: string)"),
                    new MaterializedViewSymbol("V", "(a: long, b: long)", "")),
                ".create-or-alter materialized-view V on table T { T | extend z=1.0 }",
                globals =>
                {
                    var v = globals.Database.GetMaterializedView("V");
                    Assert.IsNotNull(v);
                    Assert.AreEqual("(x: long, y: string, z: real)", v.ToTestString());
                });
        }

        [TestMethod]
        public void TestDropMaterializedView()
        {
            // drop existing materialized view
            TestApply(
                _globals.AddOrUpdateDatabaseMembers(
                    new TableSymbol("T", "(x: long, y: string)"),
                    new MaterializedViewSymbol("V", "(a: long, b: long)", "")),
                ".drop materialized-view V",
                globals =>
                {
                    var v = globals.Database.GetMaterializedView("V");
                    Assert.IsNull(v);
                });

            // drop non-existing materialized view fails
            TestApplyFails(
                _globals.AddOrUpdateDatabaseMembers(
                    new TableSymbol("T", "(x: long, y: string)")),
                ".drop materialized-view V"
                );
        }

        [TestMethod]
        public void TestRenameMaterializedView()
        {
            // rename existing materialized-view
            TestApply(
                _globals.AddOrUpdateDatabaseMembers(
                    new MaterializedViewSymbol("V", "(a: long, b: long)", "")),
                ".rename materialized-view V to Vee",
                globals =>
                {
                    var vee = globals.Database.GetMaterializedView("Vee");
                    Assert.IsNotNull(vee);
                    Assert.AreEqual("(a: long, b: long)", vee.ToTestString());
                });

            // rename non-existing materialized-view fails
            TestApplyFails(
                _globals,
                ".rename materialized-view V to Vee"
                );

            // rename existing materialized-view to existing name fails
            TestApplyFails(
                _globals.AddOrUpdateDatabaseMembers(
                    new MaterializedViewSymbol("V", "(a: long, b: long)", ""),
                    new MaterializedViewSymbol("Vee", "(x: long, y: long)", "")),
                ".rename materialized-view V to Vee"
                );
        }

        #endregion

        #region Script
        
        [TestMethod]
        public void TestExecuteDatabaseScript()
        {
            TestApply(
                _globals,
                """
                .execute script <| 
                  .create table T(x: long, y: string);
                  .create function F() { T };      
                """,
                globals =>
                {
                    Assert.AreEqual(1, globals.Database.Tables.Count);
                    Assert.AreEqual(1, globals.Database.Functions.Count);
                });
        }

        #endregion

        #region Other

        [TestMethod]
        public void TestApplyMultipleCommands()
        {
            TestApply(
                _globals,
                new[]
                {
                    ".create table T (x: long, y: string)",
                    ".set T2 <| T | extend z=1.0" 
                },
                globals =>
                {
                    var t = globals.Database.GetTable("T");
                    Assert.IsNotNull(t);
                    Assert.AreEqual("(x: long, y: string)", t.ToTestString());

                    var t2 = globals.Database.GetTable("T2");
                    Assert.IsNotNull(t2);
                    Assert.AreEqual("(x: long, y: string, z: real)", t2.ToTestString());
                });
        }

        [TestMethod]
        public void TestNames_BrackettedDeclared()
        {
            TestApply(
                _globals,
                ".create table ['T'] (['x']: long, ['y']: string)",
                globals =>
                {
                    var t = globals.Database.GetTable("T");
                    Assert.IsNotNull(t);
                    Assert.AreEqual("(x: long, y: string)", t.ToTestString());
                });
        }

        [TestMethod]
        public void TestNames_BrackettedReferenced()
        {
            TestApply(
                _globals.AddOrUpdateDatabaseMembers(new TableSymbol("T", "(a: long, b: string)")),
                ".alter table ['T'] (['x']: long, ['y']: string)",
                globals =>
                {
                    var t = globals.Database.GetTable("T");
                    Assert.IsNotNull(t);
                    Assert.AreEqual("(x: long, y: string)", t.ToTestString());
                });
        }

        [TestMethod]
        public void TestNames_String()
        {
            TestApply(
                _globals,
                ".create table 'T' (x: long, y: string)",
                globals =>
                {
                    var t = globals.Database.GetTable("T");
                    Assert.IsNotNull(t);
                    Assert.AreEqual("(x: long, y: string)", t.ToTestString());
                });
        }

        [TestMethod]
        public void TestUnhandledCommand_Fails()
        {
            TestApplyFails(
                _globals,
                ".show tables"
                );
        }

        [TestMethod]
        public void TestSkipUnhandled()
        {
            TestApply(
                _globals,
                ".show tables",
                globals =>
                {
                    Assert.AreSame(_globals, globals);
                },
                kind: ApplyKind.SkipUnhandled
                );
        }

        [TestMethod]
        public void TestSkipFailures()
        {
            TestApply(
                _globals,
                ".glurp glob",
                globals =>
                {
                    Assert.AreSame(_globals, globals);
                },
                kind: ApplyKind.SkipFailures
                );
        }

        #endregion
    }
}
