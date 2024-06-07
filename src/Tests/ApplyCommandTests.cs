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

        [TestMethod]
        public void TestCreateFunction()
        {
            // creating a function that does not exist adds the function
            var globals = _globals.ApplyCommand(".create function F(x: string) { T }");
            var fn = globals.Database.GetFunction("F");
            Assert.IsNotNull(fn);

            // creating a function that already exists replaces the existing function.. same as alter
            var globals2 = globals.ApplyCommand(".create function F() { T }");
            var fn2 = globals2.GetFunction("F");
            Assert.AreNotSame(fn, fn2);
        }

        [TestMethod]
        public void TestCreateFunction_docstring()
        {
            var globals = _globals.ApplyCommand(".create function with (docstring = 'description') F() { T }");
            var f = globals.Database.GetFunction("F");
            Assert.IsNotNull(f);
            Assert.AreEqual("description", f.Description);
        }

        [TestMethod]
        public void TestCreateFunction_ifnotexists()
        {
            var globals1 = _globals.ApplyCommand(".create function ifnotexists F(x: string) { T }");
            var fn1 = globals1.Database.GetFunction("F");
            Assert.IsNotNull(fn1);

            var globals2 = globals1.ApplyCommand(".create function ifnotexists F(x: string) { T }");
            var fn2 = globals2.Database.GetFunction("F");
            Assert.IsNotNull(fn2);
            Assert.AreSame(fn1, fn2);
        }

        [TestMethod]
        public void TestAlterFunction()
        {
            var globals = _globals.ApplyCommand(".alter function F(x: string) { T }");
            var fn = globals.Database.GetFunction("F");
            Assert.IsNull(fn);

            var globals2 = globals.AddOrUpdateDatabaseMembers(
                new FunctionSymbol("F", "(x: string)", "{ T }"));
            var fn2 = globals2.Database.GetFunction("F");
            Assert.IsNotNull(fn2);
            Assert.AreEqual(1, fn2.Signatures[0].Parameters.Count);

            var globals3 = globals2.ApplyCommand(".alter function F() { T }");
            var fn3 = globals3.Database.GetFunction("F");
            Assert.IsNotNull(fn3);
            Assert.AreNotSame(fn2, fn3);
            Assert.AreEqual(0, fn3.Signatures[0].Parameters.Count);
        }

        [TestMethod]
        public void TestAlterFunction_docstring()
        {
            var globals = _globals.AddOrUpdateDatabaseMembers(
                new FunctionSymbol("F", "()", "{ T }"));

            var globals2 = globals.ApplyCommand(".alter function with (docstring = 'description') F() { T }");
            var f = globals2.Database.GetFunction("F");
            Assert.IsNotNull(f);
            Assert.AreEqual("description", f.Description);
        }

        [TestMethod]
        public void TestAlterFunctionDocString()
        {
            var globals = _globals.AddOrUpdateDatabaseMembers(
                new FunctionSymbol("F", "()", "{ T }"));

            var globals2 = globals.ApplyCommand(".alter function F docstring 'description'");
            var fn = globals2.Database.GetFunction("F");
            Assert.IsNotNull(fn);
            Assert.AreEqual("description", fn.Description);
        }

        [TestMethod]
        public void TestCreateOrAlterFunction()
        {
            var globals = _globals.ApplyCommand(".create-or-alter function F(x: string) { T }");
            var fn = globals.Database.GetFunction("F");
            Assert.IsNotNull(fn);
            Assert.AreEqual(1, fn.Signatures[0].Parameters.Count);

            var globals2 = globals.ApplyCommand(".create-or-alter function F() { T }");
            var fn2 = globals2.Database.GetFunction("F");
            Assert.IsNotNull(fn2);
            Assert.AreNotSame(fn, fn2);
            Assert.AreEqual(0, fn2.Signatures[0].Parameters.Count);
        }

        [TestMethod]
        public void TestCreateOrAlterFunction_docstring()
        {
            var globals = _globals.ApplyCommand(".create-or-alter function with (docstring = 'description') F(x: string) { T }");
            var fn = globals.Database.GetFunction("F");
            Assert.IsNotNull(fn);
            Assert.AreEqual("description", fn.Description);
        }

        [TestMethod]
        public void TestCreateTable()
        {
            var globals = _globals.ApplyCommand(".create table T (x: long, y: string)");
            Assert.AreEqual(1, globals.Database.Tables.Count);
        }

        [TestMethod]
        public void TestCreateTable_docstring()
        {
            var globals = _globals.ApplyCommand(".create table T (x: long, y: string) with (docstring = 'description')");
            var t = globals.Database.GetTable("T");
            Assert.IsNotNull(t);
            Assert.AreEqual("description", t.Description);
        }

        [TestMethod]
        public void TestCreateTables()
        {
            var globals = _globals.ApplyCommand(".create tables T1(x: long, y: string), T2(p: long, q: string)");
            Assert.AreEqual(2, globals.Database.Tables.Count);
        }

        [TestMethod]
        public void TestCreateTables_docstring()
        {
            var globals = _globals.ApplyCommand(".create tables T1(x: long, y: string), T2(p: long, q: string) with (docstring = 'description')");
            Assert.AreEqual(2, globals.Database.Tables.Count);
            Assert.IsTrue(globals.Database.Tables.All(t => t.Description == "description"));
        }

        [TestMethod]
        public void TestCreateMergeTable_NoExistingTable()
        {
            var globals = _globals.ApplyCommand(".create-merge table T (x: long, y: string)");
            Assert.AreEqual(1, globals.Database.Tables.Count);
            Assert.AreEqual(2, globals.Database.Tables[0].Columns.Count);
        }

        [TestMethod]
        public void TestCreateMergeTable_ExistingTable()
        {
            var globals = _globals.AddOrUpdateDatabaseMembers(
                new TableSymbol("T", "(a: long, b: long)"));

            globals = globals.ApplyCommand(".create-merge table T (b: long, c: long)");

            Assert.AreEqual(1, globals.Database.Tables.Count);
            Assert.AreEqual(3, globals.Database.Tables[0].Columns.Count);
        }

        [TestMethod]
        public void TestCreateMergeTable_ExistingTable_AmbiguousColumn()
        {
            var globals = _globals.AddOrUpdateDatabaseMembers(
                new TableSymbol("T", "(a: long, b: long)"));

            globals = globals.ApplyCommand(".create-merge table T (b: string, c: long)");

            Assert.AreEqual(1, globals.Database.Tables.Count);
            Assert.AreEqual(2, globals.Database.Tables[0].Columns.Count);
        }

        [TestMethod]
        public void TestCreateMergeTable_docstring()
        {
            var globals = _globals.ApplyCommand(".create-merge table T (x: long, y: string) with (docstring = 'description')");
            var t = globals.Database.GetTable("T");
            Assert.IsNotNull(t);
            Assert.AreEqual("description", t.Description);
        }

        [TestMethod]
        public void TestCreateMergeTables()
        {
            var globals = _globals.AddOrUpdateDatabaseMembers(
                new TableSymbol("T1", "(x: long, z: real)"));
            var globals2 = globals.ApplyCommand(".create-merge tables T1(x: long, y: string), T2(p: long, q: string)");
            Assert.AreEqual(2, globals2.Database.Tables.Count);
            var t1 = globals2.Database.GetTable("T1");
            Assert.IsNotNull(t1);
            Assert.AreEqual(3, t1.Columns.Count);
            var t2 = globals2.Database.GetTable("T2");
            Assert.IsNotNull(t2);
            Assert.AreEqual(2, t2.Columns.Count);
        }

        [TestMethod]
        public void TestAlterTable()
        {
            var globals = _globals.AddOrUpdateDatabaseMembers(
                new TableSymbol("T", "(x: long, y: string)"));

            var globals2 = globals.ApplyCommand(".alter table T (x: long, z: real)");
            var t = globals2.Database.GetTable("T");
            Assert.IsNotNull(t);
            Assert.AreEqual(2, t.Columns.Count);
            var z = t.GetColumn("z");
            Assert.IsNotNull(z);
        }

        [TestMethod]
        public void TestAlterTableDocString_docstring()
        {
            var globals = _globals.AddOrUpdateDatabaseMembers(
                new TableSymbol("T", "(x: long, y: string)"));

            var globals2 = globals.ApplyCommand(".alter table T (x: long, y: string) with (docstring = 'description')");
            var t = globals2.Database.GetTable("T");
            Assert.IsNotNull(t);
            Assert.AreEqual("description", t.Description);
        }

        [TestMethod]
        public void TestAlterTableDocString()
        {
            var globals = _globals.AddOrUpdateDatabaseMembers(
                new TableSymbol("T", "(x: long, y: string)"));

            var globals2 = globals.ApplyCommand(".alter table T docstring 'description'");
            var t = globals2.Database.GetTable("T");
            Assert.IsNotNull(t);
            Assert.AreEqual("description", t.Description);
        }

        [TestMethod]
        public void TestAlterMergeTable()
        {
            var globals = _globals.AddOrUpdateDatabaseMembers(
                new TableSymbol("T", "(x: long, y: string)"));

            var globals2 = globals.ApplyCommand(".alter-merge table T (x: long, z: real)");
            var t = globals2.Database.GetTable("T");
            Assert.IsNotNull(t);
            Assert.AreEqual(3, t.Columns.Count);
        }

        [TestMethod]
        public void TestAlterMergeTable_docstring()
        {
            var globals = _globals.AddOrUpdateDatabaseMembers(
                new TableSymbol("T", "(x: long, y: string)"));

            var globals2 = globals.ApplyCommand(".alter-merge table T (x: long, z: real) with (docstring = 'description')");
            var t = globals2.Database.GetTable("T");
            Assert.IsNotNull(t);
            Assert.AreEqual("description", t.Description);
        }

        [TestMethod]
        public void TestRenameTable()
        {
            var globals = _globals.AddOrUpdateDatabaseMembers(
                new TableSymbol("T", "(x: long, y: string)"));

            var globals2 = globals.ApplyCommand(".rename table T to Tee");
            Assert.AreEqual(1, globals2.Database.Tables.Count);
            var tee = globals2.Database.GetTable("Tee");
            Assert.IsNotNull(tee);
        }

        [TestMethod]
        public void TestRenameTables()
        {
            var globals = _globals.AddOrUpdateDatabaseMembers(
                new TableSymbol("T1", "(x: long, y: string)"),
                new TableSymbol("T2", "(a: long, b: real, c: string)"));

            var globals2 = globals.ApplyCommand(".rename tables TeeOne=T1, TeeTwo=T2");
            Assert.AreEqual(2, globals2.Database.Tables.Count);
            var teeOne = globals2.Database.GetTable("TeeOne");
            Assert.IsNotNull(teeOne);
            var teeTwo = globals2.Database.GetTable("TeeTwo");
            Assert.IsNotNull(teeTwo);
        }

        [TestMethod]
        public void TestDropTable()
        {
            var globals = _globals.AddOrUpdateDatabaseMembers(
                new TableSymbol("T", "(x: long, y: string)"));

            var globals2 = globals.ApplyCommand(".drop table T");
            Assert.AreEqual(0, globals2.Database.Tables.Count);
        }

        [TestMethod]
        public void TestDropTables()
        {
            var globals = _globals.AddOrUpdateDatabaseMembers(
                new TableSymbol("T1", "(x: long, y: string)"),
                new TableSymbol("T2", "(a: long, b: real, c: string)"));

            var globals2 = globals.ApplyCommand(".drop tables (T1, T2)");
            Assert.AreEqual(0, globals2.Database.Tables.Count);
        }

        [TestMethod]
        public void TestSetTable()
        {
            var globals = _globals.AddOrUpdateDatabaseMembers(
                new TableSymbol("T", "(x: long, y: string)"));

            var globals2 = globals.ApplyCommand(".set T2 <| T");
            var t2 = globals2.Database.GetTable("T2");
            Assert.IsNotNull(t2);
            Assert.AreEqual("(x: long, y: string)", t2.ToTestString());
        }

        [TestMethod]
        public void TestSetTable_docstring()
        {
            var globals = _globals.AddOrUpdateDatabaseMembers(
                new TableSymbol("T", "(x: long, y: string)"));

            var globals2 = globals.ApplyCommand(".set T2 with (docstring='description') <| T");
            var t2 = globals2.Database.GetTable("T2");
            Assert.IsNotNull(t2);
            Assert.AreEqual("description", t2.Description);
        }

        [TestMethod]
        public void TestAppendTable()
        {
            var globals = _globals.AddOrUpdateDatabaseMembers(
                new TableSymbol("T", "(x: long, y: string)"));

            var globals2 = globals.ApplyCommand(".append T <| T extend z=1.0");
            var t = globals2.Database.GetTable("T");
            Assert.IsNotNull(t);
            Assert.AreEqual("(x: long, y: string)", t.ToTestString());
        }

        [TestMethod]
        public void TestAppendTable_extend()
        {
            var globals = _globals.AddOrUpdateDatabaseMembers(
                new TableSymbol("T", "(x: long, y: string)"));

            var globals2 = globals.ApplyCommand(".append T with (extend_schema=true) <| T | extend z=1.0");
            var t = globals2.Database.GetTable("T");
            Assert.IsNotNull(t);
            Assert.AreEqual("(x: long, y: string, z: real)", t.ToTestString());
        }

        [TestMethod]
        public void TestAppendTable_docstring()
        {
            var globals = _globals.AddOrUpdateDatabaseMembers(
                new TableSymbol("T", "(x: long, y: string)"));

            var globals2 = globals.ApplyCommand(".append T with (docstring='description') <| T");
            var t = globals2.Database.GetTable("T");
            Assert.IsNotNull(t);
            Assert.AreEqual("description", t.Description);
        }

        [TestMethod]
        public void TestSetOrAppendTable_DoesNotExist()
        {
            var globals = _globals.AddOrUpdateDatabaseMembers(
                new TableSymbol("T", "(a: long, b: string, c: real)"));

            var globals2 = globals.ApplyCommand(".set-or-append T2 <| T");
            var t2 = globals2.Database.GetTable("T2");
            Assert.IsNotNull(t2);
            Assert.AreEqual("(a: long, b: string, c: real)", t2.ToTestString());
        }

        [TestMethod]
        public void TestSetOrAppendTable_Exists()
        {
            var globals = _globals.AddOrUpdateDatabaseMembers(
                new TableSymbol("T", "(a: long, b: string, c: real)"),
                new TableSymbol("T2", "(x: long, y: string)"));

            var globals2 = globals.ApplyCommand(".set-or-append T2 <| T");
            var t2 = globals2.Database.GetTable("T2");
            Assert.IsNotNull(t2);
            Assert.AreEqual("(x: long, y: string)", t2.ToTestString());
        }

        [TestMethod]
        public void TestSetOrAppendTable_Exists_extend_schema()
        {
            var globals = _globals.AddOrUpdateDatabaseMembers(
                new TableSymbol("T", "(a: long, b: string, c: real)"),
                new TableSymbol("T2", "(x: long, y: string)"));

            var globals2 = globals.ApplyCommand(".set-or-append T2 with (extend_schema=true) <| T");
            var t2 = globals2.Database.GetTable("T2");
            Assert.IsNotNull(t2);
            Assert.AreEqual("(x: long, y: string, c: real)", t2.ToTestString());
        }

        [TestMethod]
        public void TestSetOrAppendTable_docstring()
        {
            var globals = _globals.AddOrUpdateDatabaseMembers(
                new TableSymbol("T", "(a: long, b: string)"));

            var globals2 = globals.ApplyCommand(".set-or-append T2 with (docstring='description') <| T");
            var t2 = globals2.Database.GetTable("T2");
            Assert.IsNotNull(t2);
            Assert.AreEqual("description", t2.Description);
        }

        [TestMethod]
        public void TestSetOrReplaceTable_DoesNotExist()
        {
            var globals = _globals.AddOrUpdateDatabaseMembers(
                new TableSymbol("T", "(a: long, b: string)"));

            var globals2 = globals.ApplyCommand(".set-or-replace T2 <| T");
            var t2 = globals2.Database.GetTable("T2");
            Assert.IsNotNull(t2);
            Assert.AreEqual("(a: long, b: string)", t2.ToTestString());
        }

        [TestMethod]
        public void TestSetOrReplaceTable_Exists()
        {
            var globals = _globals.AddOrUpdateDatabaseMembers(
                new TableSymbol("T", "(a: long, b: string, c: real)"),
                new TableSymbol("T2", "(x: long, y: string)"));

            var globals2 = globals.ApplyCommand(".set-or-replace T2 <| T");
            var t2 = globals2.Database.GetTable("T2");
            Assert.IsNotNull(t2);
            Assert.AreEqual("(x: long, y: string)", t2.ToTestString());
        }

        [TestMethod]
        public void TestSetOrReplaceTable_Exists_extend_schema()
        {
            var globals = _globals.AddOrUpdateDatabaseMembers(
                new TableSymbol("T", "(a: long, b: string, c: real)"),
                new TableSymbol("T2", "(x: long, y: string)"));

            var globals2 = globals.ApplyCommand(".set-or-replace T2 with (extend_schema=true) <| T");
            var t2 = globals2.Database.GetTable("T2");
            Assert.IsNotNull(t2);
            Assert.AreEqual("(x: long, y: string, c: real)", t2.ToTestString());
        }

        [TestMethod]
        public void TestSetOrReplaceTable_Exists_recreate_schema()
        {
            var globals = _globals.AddOrUpdateDatabaseMembers(
                new TableSymbol("T", "(a: long, b: string, c: real)"),
                new TableSymbol("T2", "(x: long, y: string)"));

            var globals2 = globals.ApplyCommand(".set-or-replace T2 with (recreate_schema=true) <| T");
            var t2 = globals2.Database.GetTable("T2");
            Assert.IsNotNull(t2);
            Assert.AreEqual("(a: long, b: string, c: real)", t2.ToTestString());
        }

        [TestMethod]
        public void TestSetOrReplaceTable_docstring()
        {
            var globals = _globals.AddOrUpdateDatabaseMembers(
                new TableSymbol("T", "(x: long, y: string)"));

            var globals2 = globals.ApplyCommand(".set-or-replace T2 with (docstring='description') <| T");
            var t2 = globals2.Database.GetTable("T2");
            Assert.IsNotNull(t2);
            Assert.AreEqual("description", t2.Description);
        }

        [TestMethod]
        public void TestAlterColumnType_DefaultDB()
        {
            var globals = _globals.AddOrUpdateDatabaseMembers(
                new TableSymbol("T", "(x: long, y: string)"));

            var globals2 = globals.ApplyCommand(".alter column T.y type = real");
            var t = globals2.Database.GetTable("T");
            Assert.IsNotNull(t);
            Assert.AreEqual("(x: long, y: real)", t.ToTestString());
        }

        [TestMethod]
        public void TestAlterColumnType_DB()
        {
            var globals = _globals
                .AddOrUpdateClusterDatabase(
                    new DatabaseSymbol("db2",
                        new TableSymbol("T", "(x: long, y: string)")));

            Assert.AreEqual(2, globals.Cluster.Databases.Count);

            var globals2 = globals.ApplyCommand(".alter column ['db2'].T.y type = real");
            var t = globals2.Cluster.GetDatabase("db2").GetTable("T");
            Assert.IsNotNull(t);
            Assert.AreEqual("(x: long, y: real)", t.ToTestString());
        }

        [TestMethod]
        public void TestDropColumn()
        {
            var globals = _globals.AddOrUpdateDatabaseMembers(
                new TableSymbol("T", "(x: long, y: string)"));

            var globals2 = globals.ApplyCommand(".drop column T.y");
            var t = globals2.Database.GetTable("T");
            Assert.IsNotNull(t);
            Assert.AreEqual("(x: long)", t.ToTestString());
        }

        [TestMethod]
        public void TestDropTableColumns()
        {
            var globals = _globals.AddOrUpdateDatabaseMembers(
                new TableSymbol("T", "(x: long, y: string, z: real)"));

            var globals2 = globals.ApplyCommand(".drop table T columns (x, z)");
            var t = globals2.Database.GetTable("T");
            Assert.IsNotNull(t);
            Assert.AreEqual("(y: string)", t.ToTestString());
        }

        [TestMethod]
        public void TestRenameColumn()
        {
            var globals = _globals.AddOrUpdateDatabaseMembers(
                new TableSymbol("T", "(x: long, y: string)"));

            var globals2 = globals.ApplyCommand(".rename column T.y to why");
            var t = globals2.Database.GetTable("T");
            Assert.IsNotNull(t);
            Assert.AreEqual("(x: long, why: string)", t.ToTestString());
        }

        [TestMethod]
        public void TestRenameColumns()
        {
            var globals = _globals
                .AddOrUpdateClusterDatabases(
                    new DatabaseSymbol("db2",
                        new TableSymbol("T2", "(x: long, y: string)")))
                .AddOrUpdateDatabaseMembers(
                    new TableSymbol("T", "(x: long, y: string)"));

            var globals2 = globals.ApplyCommand(".rename columns why=T.y, why2=db2.T2.y");
            var t = globals2.Database.GetTable("T");
            Assert.IsNotNull(t);
            Assert.AreEqual("(x: long, why: string)", t.ToTestString());
            var db2 = globals2.Cluster.GetDatabase("db2");
            Assert.IsNotNull(db2);
            var t2 = db2.GetTable("T2");
            Assert.IsNotNull(t2);
            Assert.AreEqual("(x: long, why2: string)", t2.ToTestString());
        }

        [TestMethod]
        public void TestAlterTableColumnDocStrings()
        {
            var globals = _globals
                .AddOrUpdateDatabaseMembers(
                    new TableSymbol("T", "(x: long, y: string)")
                        .AddOrUpdateColumns(new ColumnSymbol("z", ScalarTypes.Real, "z description")));

            var globals2 = globals.ApplyCommand(".alter table T column-docstrings (x: 'x description', y: 'y description')");
            var t = globals2.Database.GetTable("T");
            Assert.IsNotNull(t);
            Assert.AreEqual(3, t.Columns.Count);
            Assert.AreEqual("x description", t.Columns[0].Description);
            Assert.AreEqual("y description", t.Columns[1].Description);
            Assert.AreEqual("", t.Columns[2].Description);
        }

        [TestMethod]
        public void TestAlterMergeTableColumnDocStrings()
        {
            var globals = _globals
                .AddOrUpdateDatabaseMembers(
                    new TableSymbol("T", "(x: long, y: string)")
                        .AddOrUpdateColumns(new ColumnSymbol("z", ScalarTypes.Real, "z description")));

            var globals2 = globals.ApplyCommand(".alter-merge table T column-docstrings (x: 'x description', y: 'y description')");
            var t = globals2.Database.GetTable("T");
            Assert.IsNotNull(t);
            Assert.AreEqual(3, t.Columns.Count);
            Assert.AreEqual("x description", t.Columns[0].Description);
            Assert.AreEqual("y description", t.Columns[1].Description);
            Assert.AreEqual("z description", t.Columns[2].Description);
        }

        [TestMethod]
        public void TestCreateStorageExternalTable_DoesNotExist()
        {
            var globals = _globals.ApplyCommand(".create external table ET (x: long, y: string) kind=storage dataformat=json ('connection')");
            var et = globals.Database.GetExternalTable("ET");
            Assert.IsNotNull(et);
            Assert.AreEqual("(x: long, y: string)", et.ToTestString());
        }

        [TestMethod]
        public void TestCreateStorageExternalTable_Exists()
        {
            var globals = _globals.AddOrUpdateDatabaseMembers(
                new ExternalTableSymbol("ET", "(a: long, b: string)"));

            var globals2 = globals.ApplyCommand(".create external table ET (x: long, y: string) kind=storage dataformat=json ('connection')");
            var et = globals2.Database.GetExternalTable("ET");
            Assert.IsNotNull(et);
            Assert.AreEqual("(a: long, b: string)", et.ToTestString());
        }

        [TestMethod]
        public void TestCreateStorageExternalTable_docstring()
        {
            var globals = _globals.ApplyCommand(".create external table ET (x: long, y: string) kind=storage dataformat=json ('connection') with (docstring='description')");
            var et = globals.Database.GetExternalTable("ET");
            Assert.IsNotNull(et);
            Assert.AreEqual("description", et.Description);
        }

        [TestMethod]
        public void TestAlterStorageExternalTable_Exists()
        {
            var globals = _globals.AddOrUpdateDatabaseMembers(
                new ExternalTableSymbol("ET", "(a: long, b: string)"));

            var globals2 = globals.ApplyCommand(".alter external table ET (x: long, y: string) kind=storage dataformat=json ('connection')");
            var et = globals2.Database.GetExternalTable("ET");
            Assert.IsNotNull(et);
            Assert.AreEqual("(x: long, y: string)", et.ToTestString());
        }

        [TestMethod]
        public void TestAlterStorageExternalTable_DoesNotExist()
        {
            var globals = _globals;
            var globals2 = globals.ApplyCommand(".alter external table ET (x: long, y: string) kind=storage dataformat=json ('connection')");
            var et = globals2.Database.GetExternalTable("ET");
            Assert.IsNull(et);
        }

        [TestMethod]
        public void TestAlterStorageExternalTable_docstring()
        {
            var globals = _globals.AddOrUpdateDatabaseMembers(
                new ExternalTableSymbol("ET", "(a: long, b: string)"));

            var globals2 = globals.ApplyCommand(".alter external table ET (x: long, y: string) kind=storage dataformat=json ('connection') with (docstring='description')");
            var et = globals2.Database.GetExternalTable("ET");
            Assert.IsNotNull(et);
            Assert.AreEqual("description", et.Description);
        }

        [TestMethod]
        public void TestCreateOrAlterStorageExternalTable_Exists()
        {
            var globals = _globals.AddOrUpdateDatabaseMembers(
                new ExternalTableSymbol("ET", "(a: long, b: string)"));

            var globals2 = globals.ApplyCommand(".create-or-alter external table ET (x: long, y: string) kind=storage dataformat=json ('connection')");
            var et = globals2.Database.GetExternalTable("ET");
            Assert.IsNotNull(et);
            Assert.AreEqual("(x: long, y: string)", et.ToTestString());
        }

        [TestMethod]
        public void TestCreateOrAlterStorageExternalTable_DoesNotExist()
        {
            var globals = _globals;
            var globals2 = globals.ApplyCommand(".create-or-alter external table ET (x: long, y: string) kind=storage dataformat=json ('connection')");
            var et = globals2.Database.GetExternalTable("ET");
            Assert.IsNotNull(et);
            Assert.AreEqual("(x: long, y: string)", et.ToTestString());
        }

        [TestMethod]
        public void TestCreateOrAlterStorageExternalTable_docstring()
        {
            var globals = _globals;
            var globals2 = globals.ApplyCommand(".create-or-alter external table ET (x: long, y: string) kind=storage dataformat=json ('connection') with (docstring='description')");
            var et = globals2.Database.GetExternalTable("ET");
            Assert.IsNotNull(et);
            Assert.AreEqual("description", et.Description);
        }

        [TestMethod]
        public void TestCreateSqlExternalTable()
        {
            var globals = _globals.ApplyCommand(".create external table ET (x: long, y: string) kind=sql table=Customers ('connection')");
            var et = globals.Database.GetExternalTable("ET");
            Assert.IsNotNull(et);
            Assert.AreEqual("(x: long, y: string)", et.ToTestString());
        }

        [TestMethod]
        public void TestAlterSqlExternalTable()
        {
            var globals = _globals.AddOrUpdateDatabaseMembers(
                new ExternalTableSymbol("ET", "(a: long, b: string)"));
            var globals2 = globals.ApplyCommand(".alter external table ET (x: long, y: string) kind=sql table=Customers ('connection')");
            var et = globals2.Database.GetExternalTable("ET");
            Assert.IsNotNull(et);
            Assert.AreEqual("(x: long, y: string)", et.ToTestString());
        }

        [TestMethod]
        public void TestCreateOrAlterSqlExternalTable()
        {
            var globals = _globals.AddOrUpdateDatabaseMembers(
                new ExternalTableSymbol("ET", "(a: long, b: string)"));
            var globals2 = globals.ApplyCommand(".create-or-alter external table ET (x: long, y: string) kind=sql table=Customers ('connection')");
            var et = globals2.Database.GetExternalTable("ET");
            Assert.IsNotNull(et);
            Assert.AreEqual("(x: long, y: string)", et.ToTestString());
        }

        [TestMethod]
        public void TestDropExternalTable()
        {
            var globals = _globals.AddOrUpdateDatabaseMembers(
                new ExternalTableSymbol("ET", "(a: long, b: string)"));
            var globals2 = globals.ApplyCommand(".drop external table ET");
            var et = globals2.Database.GetExternalTable("ET");
            Assert.IsNull(et);
        }

        [TestMethod]
        public void TestCreateMaterializedView_DoesNotExist()
        {
            var globals = _globals.AddOrUpdateDatabaseMembers(
                new TableSymbol("T", "(x: long, y: string)"));

            var globals2 = globals.ApplyCommand(".create materialized-view V on table T { T | extend z=1.0 }");
            var v = globals2.Database.GetMaterializedView("V");
            Assert.IsNotNull(v);
            Assert.AreEqual("(x: long, y: string, z: real)", v.ToTestString());
        }

        [TestMethod]
        public void TestCreateMaterializedView_Exists()
        {
            var globals = _globals.AddOrUpdateDatabaseMembers(
                new TableSymbol("T", "(x: long, y: string)"),
                new MaterializedViewSymbol("V", "(x: long, y: string)", ""));

            var globals2 = globals.ApplyCommand(".create materialized-view V on table T { T | extend z=1.0 }");
            var v = globals2.Database.GetMaterializedView("V");
            Assert.IsNotNull(v);
            Assert.AreEqual("(x: long, y: string)", v.ToTestString());
        }

        [TestMethod]
        public void TestCreateMaterializedView_docstring()
        {
            var globals = _globals.AddOrUpdateDatabaseMembers(
                new TableSymbol("T", "(x: long, y: string)"));

            var globals2 = globals.ApplyCommand(".create materialized-view with (docstring='description') V on table T { T | extend z=1.0 }");
            var v = globals2.Database.GetMaterializedView("V");
            Assert.IsNotNull(v);
            Assert.AreEqual("description", v.Description);
        }

        [TestMethod]
        public void TestAlterMaterializedView_DoesNotExist()
        {
            var globals = _globals.AddOrUpdateDatabaseMembers(
                new TableSymbol("T", "(x: long, y: string)"));

            var globals2 = globals.ApplyCommand(".alter materialized-view V on table T { T | extend z=1.0 }");
            var v = globals2.Database.GetMaterializedView("V");
            Assert.IsNull(v);
        }

        [TestMethod]
        public void TestAlterMaterializedView_Exists()
        {
            var globals = _globals.AddOrUpdateDatabaseMembers(
                new TableSymbol("T", "(x: long, y: string)"),
                new MaterializedViewSymbol("V", "(a: long, b: long)", ""));

            var globals2 = globals.ApplyCommand(".alter materialized-view V on table T { T | extend z=1.0 }");
            var v = globals2.Database.GetMaterializedView("V");
            Assert.IsNotNull(v);
            Assert.AreEqual("(x: long, y: string, z: real)", v.ToTestString());
        }

        [TestMethod]
        public void TestCreateOrAlterMaterializedView_DoesNotExist()
        {
            var globals = _globals.AddOrUpdateDatabaseMembers(
                new TableSymbol("T", "(x: long, y: string)"));

            var globals2 = globals.ApplyCommand(".create-or-alter materialized-view V on table T { T | extend z=1.0 }");
            var v = globals2.Database.GetMaterializedView("V");
            Assert.IsNotNull(v);
            Assert.AreEqual("(x: long, y: string, z: real)", v.ToTestString());
        }

        [TestMethod]
        public void TestCreateOrAlterMaterializedView_Exists()
        {
            var globals = _globals.AddOrUpdateDatabaseMembers(
                new TableSymbol("T", "(x: long, y: string)"),
                new MaterializedViewSymbol("V", "(a: long, b: long)", ""));

            var globals2 = globals.ApplyCommand(".create-or-alter materialized-view V on table T { T | extend z=1.0 }");
            var v = globals2.Database.GetMaterializedView("V");
            Assert.IsNotNull(v);
            Assert.AreEqual("(x: long, y: string, z: real)", v.ToTestString());
        }

        [TestMethod]
        public void TestAlterMaterializedViewDocString()
        {
            var globals = _globals.AddOrUpdateDatabaseMembers(
                new TableSymbol("T", "(x: long, y: string)"),
                new MaterializedViewSymbol("V", "(a: long, b: long)", ""));

            var globals2 = globals.ApplyCommand(".alter materialized-view V docstring 'description'");
            var v = globals2.Database.GetMaterializedView("V");
            Assert.IsNotNull(v);
            Assert.AreEqual("description", v.Description);
        }

        [TestMethod]
        public void TestDropMaterializedView()
        {
            var globals = _globals.AddOrUpdateDatabaseMembers(
                new TableSymbol("T", "(x: long, y: string)"),
                new MaterializedViewSymbol("V", "(a: long, b: long)", ""));

            var globals2 = globals.ApplyCommand(".drop materialized-view V");
            var v = globals2.Database.GetMaterializedView("V");
            Assert.IsNull(v);
        }

        [TestMethod]
        public void TestRenameMaterializedView()
        {
            var globals = _globals.AddOrUpdateDatabaseMembers(
                new TableSymbol("T", "(x: long, y: string)"),
                new MaterializedViewSymbol("V", "(a: long, b: long)", ""));

            var globals2 = globals.ApplyCommand(".rename materialized-view V to Vee");
            var vee = globals2.Database.GetMaterializedView("Vee");
            Assert.IsNotNull(vee);
            Assert.AreEqual("(a: long, b: long)", vee.ToTestString());
        }

        [TestMethod]
        public void TestExecuteDatabaseScript()
        {
            var globals = _globals.ApplyCommand(
                """
                .execute script <| 
                  .create table T(x: long, y: string);
                  .create function F() { T };                
                """);

            Assert.AreEqual(1, globals.Database.Tables.Count);
            Assert.AreEqual(1, globals.Database.Functions.Count);
        }

        [TestMethod]
        public void TestApplyMultipleCommands()
        {
            var globals = _globals.ApplyCommands(
                ".create table T (x: long, y: string)",
                ".set T2 <| T | extend z=1.0");

            var t = globals.Database.GetTable("T");
            Assert.IsNotNull(t);
            Assert.AreEqual("(x: long, y: string)", t.ToTestString());

            var t2 = globals.Database.GetTable("T2");
            Assert.IsNotNull(t2);
            Assert.AreEqual("(x: long, y: string, z: real)", t2.ToTestString());
        }

        [TestMethod]
        public void TestNames_BrackettedDeclared()
        {
            var globals = _globals.ApplyCommand(".create table ['T'] (['x']: long, ['y']: string)");
            var t = globals.Database.GetTable("T");
            Assert.IsNotNull(t);
            Assert.AreEqual("(x: long, y: string)", t.ToTestString());
        }

        [TestMethod]
        public void TestNames_BrackettedReferenced()
        {
            var globals = _globals
                .AddOrUpdateDatabaseMembers(new TableSymbol("T", "(a: long, b: string)"))
                .ApplyCommand(".alter table ['T'] (['x']: long, ['y']: string)");
            var t = globals.Database.GetTable("T");
            Assert.IsNotNull(t);
            Assert.AreEqual("(x: long, y: string)", t.ToTestString());
        }

        [TestMethod]
        public void TestNames_String()
        {
            var globals = _globals.ApplyCommand(".create table 'T' (x: long, y: string)");
            var t = globals.Database.GetTable("T");
            Assert.IsNotNull(t);
            Assert.AreEqual("(x: long, y: string)", t.ToTestString());
        }
    }
}
