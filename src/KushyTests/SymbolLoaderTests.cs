using System;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Kusto.Language;
using Kusto.Language.Syntax;
using Kusto.Language.Editor;
using Kushy;

namespace KushyTests
{
    [TestClass]
    public class SymbolLoaderTests
    {
        private static readonly string HelpConnection = "https://help.kusto.windows.net;Fed=true";

        [TestMethod]
        public async Task TestLoadDatabaseAsync()
        {
            var loader = new SymbolLoader(HelpConnection);
            var dbSymbol = await loader.LoadDatabaseAsync("Samples");
            Assert.IsNotNull(dbSymbol);
            Assert.IsTrue(dbSymbol.Members.Count > 0, "members count");
            Assert.IsTrue(dbSymbol.Tables.Count > 0, "tables count");
            Assert.IsTrue(dbSymbol.MaterializedViews.Count > 0, "materialized views count");
            Assert.IsTrue(dbSymbol.Functions.Count > 0, "functions count");
        }

        [TestMethod]
        public async Task TestLoadDatabaseAsync_BadDatabaseName()
        {
            var loader = new SymbolLoader(HelpConnection);
            var dbSymbol = await loader.LoadDatabaseAsync("not-a-db");
            Assert.IsNull(dbSymbol);
        }

        [TestMethod]
        public async Task TestGetDatabaseNamesAsync()
        {
            var loader = new SymbolLoader(HelpConnection);
            var names = await loader.GetDatabaseNamesAsync();
            Assert.AreEqual(1, names.Length);
            Assert.AreEqual("Samples", names[0]);
        }

        [TestMethod]
        public async Task TestAddOrUpdateDatabaseAsync()
        {
            // no clusters, but default cluster and database are set with no names.
            var globals = GlobalState.Default;
            Assert.AreEqual(0, globals.Clusters.Count);
            Assert.IsNotNull(globals.Cluster);
            Assert.AreEqual("", globals.Cluster.Name);
            Assert.AreEqual(0, globals.Cluster.Members.Count);
            Assert.IsNotNull(globals.Database);
            Assert.AreEqual("", globals.Database.Name);
            Assert.AreEqual(0, globals.Database.Members.Count);

            // add Samples database
            var loader = new SymbolLoader(HelpConnection);
            var newGlobals = await loader.AddOrUpdateDatabaseAsync(globals, "Samples");

            // a new cluster is added with the loaded Samples database
            Assert.AreEqual(1, newGlobals.Clusters.Count);
            var cluster = newGlobals.Clusters[0];
            Assert.AreEqual("help.kusto.windows.net", cluster.Name);
            Assert.AreEqual(1, cluster.Databases.Count);
            var db = cluster.Databases[0];
            Assert.AreEqual("Samples", db.Name);
            Assert.IsTrue(db.Members.Count > 0);

            // default Cluster and Database stay the same
            Assert.IsNotNull(newGlobals.Cluster);
            Assert.AreEqual("", newGlobals.Cluster.Name);
            Assert.IsNotNull(newGlobals.Database);
            Assert.AreEqual("", newGlobals.Database.Name);
        }

        [TestMethod]
        public async Task TestAddOrUpdateDefaultDatabaseAsync()
        {
            // no clusters, but default cluster and database are set with no names.
            var globals = GlobalState.Default;
            Assert.AreEqual(0, globals.Clusters.Count);
            Assert.IsNotNull(globals.Cluster);
            Assert.AreEqual("", globals.Cluster.Name);
            Assert.AreEqual(0, globals.Cluster.Members.Count);
            Assert.IsNotNull(globals.Database);
            Assert.AreEqual("", globals.Database.Name);
            Assert.AreEqual(0, globals.Database.Members.Count);

            // add Samples database and make it the default
            var loader = new SymbolLoader(HelpConnection);
            var newGlobals = await loader.AddOrUpdateDefaultDatabaseAsync(globals, "Samples");

            // a new cluster is added with the loaded Samples database
            Assert.AreEqual(1, newGlobals.Clusters.Count);
            var cluster = newGlobals.Clusters[0];
            Assert.AreEqual("help.kusto.windows.net", cluster.Name);
            Assert.AreEqual(1, cluster.Databases.Count);
            var db = cluster.Databases[0];
            Assert.AreEqual("Samples", db.Name);
            Assert.IsTrue(db.Members.Count > 0);

            // default Cluster and Database now refer to newly loaded cluster and database.
            Assert.AreSame(cluster, newGlobals.Cluster);
            Assert.AreSame(db, newGlobals.Database);
        }

        [TestMethod]
        public async Task TestAddReferencedDatabasesAsync_KustoCode()
        {
            var loader = new SymbolLoader(HelpConnection);

            // set default database to database other than Samples.
            var globals = await loader.AddOrUpdateDefaultDatabaseAsync(GlobalState.Default, "KustoMonitoringPersistentDatabase");

            // just one database should exist
            Assert.AreEqual(1, globals.Cluster.Databases.Count);

            // parse query that has explicit reference to Samples database.
            var code = KustoCode.ParseAndAnalyze("database('Samples').StormEvents", globals);
            
            // use loader to add symbols for any explicity referenced databases
            var newCode = await loader.AddReferencedDatabasesAsync(code);

            // both databases should exist now
            Assert.AreEqual(2, newCode.Globals.Cluster.Databases.Count);

            // find StormEvents table in Samples database
            var samples = newCode.Globals.Cluster.Databases.First(db => db.Name == "Samples");
            var storm = samples.Members.First(m => m.Name == "StormEvents");

            // verify that query expression returns StormEvents table
            var qb = (QueryBlock)newCode.Syntax;
            var expr = (qb.Statements[qb.Statements.Count - 1].Element as ExpressionStatement).Expression;
            Assert.AreSame(storm, expr.ResultType);
        }

        [TestMethod]
        public async Task TestAddReferencedDatabasesAsync_CodeScript()
        {
            var loader = new SymbolLoader(HelpConnection);

            // set default database to database other than Samples.
            var globals = await loader.AddOrUpdateDefaultDatabaseAsync(GlobalState.Default, "KustoMonitoringPersistentDatabase");

            // just one database should exist
            Assert.AreEqual(1, globals.Cluster.Databases.Count);

            // create script from query and globals
            var script = CodeScript.From("database('Samples').StormEvents", globals);

            // use loader to add symbols for any explicity referenced databases
            var newScript = await loader.AddReferencedDatabasesAsync(script);

            // both databases should exist now
            Assert.AreEqual(2, newScript.Globals.Cluster.Databases.Count);

            // find StormEvents table in Samples database
            var samples = newScript.Globals.Cluster.Databases.First(db => db.Name == "Samples");
            var storm = samples.GetTable("StormEvents");
        }
    }
}
