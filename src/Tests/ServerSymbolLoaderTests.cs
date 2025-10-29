using System;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Kusto.Toolkit;

namespace Tests
{
    [TestClass]
    public class ServerSymbolLoaderTests
    {
        private async Task Test(object[] results, Func<ServerSymbolLoader, Task> fnValidate)
        {
            var loader = new TestServerSymbolLoader(results);
            await fnValidate(loader);
        }

        [TestMethod]
        public async Task TestLoadDatabaseNamesAsync()
        {
            // null databases
            await Test([],
                async (loader) =>
                {
                    var dbNames = await loader.LoadDatabaseNamesAsync();
                    Assert.IsNull(dbNames);
                });

            // empty database list
            await Test([
                new ServerSymbolLoader.DatabaseNamesResult[]{}
                ],
                async (loader) =>
                {
                    var dbNames = await loader.LoadDatabaseNamesAsync();
                    Assert.IsNotNull(dbNames);
                    Assert.AreEqual(0, dbNames.Count);
                });

            // database list
            await Test([
                new ServerSymbolLoader.DatabaseNamesResult[]{
                    new (){ DatabaseName = "db1", PrettyName="db1" },
                    new (){ DatabaseName = "db2", PrettyName="database#2" }
                }],
                async (loader) =>
                {
                    var dbNames = await loader.LoadDatabaseNamesAsync();
                    Assert.IsNotNull(dbNames);
                    Assert.AreEqual(2, dbNames.Count);
                });
        }

        [TestMethod]
        public async Task TestLoadDatabaseAsync_UnknownDatabase()
        {
            await Test([], async (loader) =>
            {
                var dbSymbol = await loader.LoadDatabaseAsync("UnknownDb");
                Assert.IsNull(dbSymbol);
            });
        }

        [TestMethod]
        public async Task TestLoadDatabaseAsync_KnownDatabase()
        {
            // call with db name
            await Test(
                [new DatabaseName("MyDb", "MyPrettyDb")],
                async (loader) =>
                {
                    var dbSymbol = await loader.LoadDatabaseAsync("MyDb");
                    Assert.IsNotNull(dbSymbol);
                    Assert.AreEqual("MyDb", dbSymbol.Name);
                    Assert.AreEqual("MyPrettyDb", dbSymbol.AlternateName);
                });

            // call with pretty name
            await Test(
                [new DatabaseName("MyDb", "MyPrettyDb")],
                async (loader) =>
                {
                    var dbSymbol = await loader.LoadDatabaseAsync("MyPrettyDb");
                    Assert.IsNotNull(dbSymbol);
                    Assert.AreEqual("MyDb", dbSymbol.Name);
                    Assert.AreEqual("MyPrettyDb", dbSymbol.AlternateName);
                });
        }

        [TestMethod]
        public async Task TestLoadDatabaseAsync_Tables()
        {
            await Test([
                new DatabaseName("MyDb", "MyPrettyDb"),
                new ServerSymbolLoader.LoadTablesResult[]
                {
                    new () {TableName="T", Schema="x:long", DocString=""},
                    new () {TableName="T2", Schema="a:string, b:real", DocString="The terminator"}
                }],
                async (loader) =>
                {
                    var dbSymbol = await loader.LoadDatabaseAsync("MyDb");
                    Assert.IsNotNull(dbSymbol);
                    Assert.AreEqual(2, dbSymbol.Members.Count);
                    Assert.IsNotNull(dbSymbol.GetTable("T"));
                    Assert.IsNotNull(dbSymbol.GetTable("T2"));
                });
        }

        [TestMethod]
        public async Task TestLoadDatabaseAsync_Functions()
        {
            await Test([
                new DatabaseName("MyDb", "MyPrettyDb"),
                new ServerSymbolLoader.LoadFunctionsResult[]
                {
                    new () { Name="F", Parameters="(x:long)", Body="{ x + 1 }" },
                }],
                async (loader) =>
                {
                    var dbSymbol = await loader.LoadDatabaseAsync("MyDb");
                    Assert.IsNotNull(dbSymbol);
                    Assert.AreEqual(1, dbSymbol.Functions.Count);
                    Assert.IsNotNull(dbSymbol.GetFunction("F"));
                });
        }

        [TestMethod]
        public async Task TestLoadDatabaseAsync_ExternalTables()
        {
            await Test(
                [
                new DatabaseName("MyDb", "MyPrettyDb"),
                new ServerSymbolLoader.LoadExternalTablesResult1[]
                {
                    new () { TableName="ET" }
                },
                new ServerSymbolLoader.LoadExternalTablesResult2[]
                {
                    new () { TableName="ET", Schema="x:long" }
                }
                ],
                async (loader) =>
                {
                    var dbSymbol = await loader.LoadDatabaseAsync("MyDb");
                    Assert.IsNotNull(dbSymbol);
                    Assert.AreEqual(1, dbSymbol.ExternalTables.Count);
                    Assert.IsNotNull(dbSymbol.GetExternalTable("ET"));
                });
        }

        [TestMethod]
        public async Task TestLoadDatabaseAsync_MaterializedViews()
        {
            await Test(
                [
                new DatabaseName("MyDb", "MyPrettyDb"),
                new ServerSymbolLoader.LoadMaterializedViewsResult1[]
                {
                    new () { Name="MV", Query="T" }
                },
                new ServerSymbolLoader.LoadMaterializedViewsResult2[]
                {
                    new () { Name="MV", Schema="x:long" }
                }
                ],
                async (loader) =>
                {
                    var dbSymbol = await loader.LoadDatabaseAsync("MyDb");
                    Assert.IsNotNull(dbSymbol);
                    Assert.AreEqual(1, dbSymbol.MaterializedViews.Count);
                    Assert.IsNotNull(dbSymbol.GetMaterializedView("MV"));
                });
        }

        [TestMethod]
        public async Task TestLoadDatabaseAsync_EntityGroups()
        {
            await Test(
                [
                new DatabaseName("MyDb", "MyPrettyDb"),
                new ServerSymbolLoader.LoadEntityGroupsResult[]
                {
                    new () { Name="EG", Entities="database('db'), database('db2')" }
                },
                ],
                async (loader) =>
                {
                    var dbSymbol = await loader.LoadDatabaseAsync("MyDb");
                    Assert.IsNotNull(dbSymbol);
                    Assert.AreEqual(1, dbSymbol.EntityGroups.Count);
                    Assert.IsNotNull(dbSymbol.GetEntityGroup("EG"));
                });
        }

        [TestMethod]
        public async Task TestLoadDatabaseAsync_GraphModels()
        {
            await Test(
                [
                new DatabaseName("MyDb", "MyPrettyDb"),
                new ServerSymbolLoader.LoadGraphModelResult[]
                {
                    new () { Name="GM", Model="""{ "Definition": { "Steps": [{ "Kind": "AddEdges", "Query": "E" }, { "Kind": "AddNodes", "Query": "N" }] } }""" }
                },
                new ServerSymbolLoader.LoadGraphModelSnapshotsResult[]
                {
                    new () { ModelName="GM", Snapshots="""["Latest", "SN1", "SN2"]""" }
                }
                ],
                async (loader) =>
                {
                    var dbSymbol = await loader.LoadDatabaseAsync("MyDb");
                    Assert.IsNotNull(dbSymbol);
                    Assert.AreEqual(1, dbSymbol.GraphModels.Count);
                    var gm = dbSymbol.GetGraphModel("GM");
                    Assert.IsNotNull(gm);
                    Assert.AreEqual(1, gm.Edges.Count);
                    Assert.AreEqual(1, gm.Nodes.Count);
                    Assert.AreEqual(3, gm.Snapshots.Count);
                });
        }
    }
}
