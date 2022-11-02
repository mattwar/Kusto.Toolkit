using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Kusto.Language;
using Kusto.Language.Syntax;
using Kusto.Language.Editor;
using Kushy;
using Kusto.Language.Symbols;

namespace KushyTests
{
    [TestClass]
    public class SymbolLoaderTests
    {
        private static readonly string HelpConnection = "https://help.kusto.windows.net;Fed=true";
        private static readonly string TestSchemaPath = "Schema";
        private static readonly string HelpCluster = "help.kusto.windows.net";

        // Use this test to update database schema files in test source directory when the server schema or serialization changes
#if false
        [TestMethod]
        public async Task Test_UpdateTestCachedSchema()
        {
            using (var loader = new ServerSymbolLoader(HelpConnection))
            {
                var samples = await loader.LoadDatabaseAsync("Samples");
                var monitoring = await loader.LoadDatabaseAsync("KustoMonitoringPersistentDatabase");

                // load schema from server and update test schema
                var fileLoader = new FileSymbolLoader($@"..\..\..\{TestSchemaPath}", HelpCluster);
                await fileLoader.SaveDatabaseAsync(samples);
                await fileLoader.SaveDatabaseAsync(monitoring);
            }
        }
#endif

        [TestMethod]
        public async Task TestServerSymbolLoader_LoadDatabaseAsync()
        {
            using (var loader = new ServerSymbolLoader(HelpConnection))
            {
                var dbSymbol = await loader.LoadDatabaseAsync("Samples");
                Assert.IsNotNull(dbSymbol);
                Assert.IsTrue(dbSymbol.Members.Count > 0, "members count");
                Assert.IsTrue(dbSymbol.Tables.Count > 0, "tables count");
                Assert.IsTrue(dbSymbol.MaterializedViews.Count > 0, "materialized views count");
                Assert.IsTrue(dbSymbol.Functions.Count > 0, "functions count");
            }
        }

        [TestMethod]
        public async Task TestServerSymbolLoader_LoadDatabaseAsync_BadDatabaseName()
        {
            using (var loader = new ServerSymbolLoader(HelpConnection))
            {
                var dbSymbol = await loader.LoadDatabaseAsync("not-a-db");
                Assert.IsNull(dbSymbol);
            }
        }

        [TestMethod]
        public async Task TestServerSymbolLoader_GetDatabaseNamesAsync()
        {
            using (var loader = new ServerSymbolLoader(HelpConnection))
            {
                var names = await loader.GetDatabaseNamesAsync();
                Assert.IsTrue(names.Contains("Samples"));
            }
        }

        [TestMethod]
        public async Task TestSymbolLoader_AddOrUpdateDatabaseAsync()
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
            using (var loader = new FileSymbolLoader(TestSchemaPath, HelpCluster))
            {
                var newGlobals = await loader.AddOrUpdateDatabaseAsync(globals, "Samples");

                // a new cluster is added with the loaded Samples database
                Assert.AreEqual(1, newGlobals.Clusters.Count);
                var cluster = newGlobals.Clusters[0];
                Assert.AreEqual(HelpCluster, cluster.Name);
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
        }

        [TestMethod]
        public async Task TestSymbolLoader_AddOrUpdateDefaultDatabaseAsync()
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
            using (var loader = new FileSymbolLoader(TestSchemaPath, HelpCluster))
            {
                var newGlobals = await loader.AddOrUpdateDefaultDatabaseAsync(globals, "Samples");

                // a new cluster is added with the loaded Samples database
                Assert.AreEqual(1, newGlobals.Clusters.Count);
                var cluster = newGlobals.Clusters[0];
                Assert.AreEqual(HelpCluster, cluster.Name);
                Assert.AreEqual(1, cluster.Databases.Count);
                var db = cluster.Databases[0];
                Assert.AreEqual("Samples", db.Name);
                Assert.IsTrue(db.Members.Count > 0);

                // default Cluster and Database now refer to newly loaded cluster and database.
                Assert.AreSame(cluster, newGlobals.Cluster);
                Assert.AreSame(db, newGlobals.Database);
            }
        }

        [TestMethod]
        public async Task TestFileSymbolLoader_SaveDatabaseAsyc_default_cluster()
        {
            var cachePath = GetTestCachePath();
            var loader = new FileSymbolLoader(cachePath, "default_cluster");

            var db = new DatabaseSymbol("db", new TableSymbol("Table", "(x: long, y: string)"));
            await loader.SaveDatabaseAsync(db);

            var clusterCachePath = loader.GetClusterCachePath();
            Assert.IsTrue(Directory.Exists(clusterCachePath), "cluster cache directory exists after saving database");

            var dbCachePath = loader.GetDatabaseCachePath(db.Name);
            Assert.IsTrue(File.Exists(dbCachePath), "database cache file exists after saving database");

            var loadedDb = await loader.LoadDatabaseAsync("db");
            Assert.IsNotNull(loadedDb);
            AssertEqual(db, loadedDb);

            loader.DeleteCache();
        }

        [TestMethod]
        public async Task TestFileSymbolLoader_SaveDatabaseAsyc_explicit_cluster()
        {
            var cachePath = GetTestCachePath();
            var loader = new FileSymbolLoader(cachePath, "default_cluster");

            var db = new DatabaseSymbol("db", new TableSymbol("Table", "(x: long, y: string)"));
            await loader.SaveDatabaseAsync(db, "cluster");

            var defaultClusterCachePath = loader.GetClusterCachePath("default_cluster");
            Assert.IsFalse(Directory.Exists(defaultClusterCachePath), "default cluster cache directory does not exists after saving database");

            var clusterCachePath = loader.GetClusterCachePath("cluster");
            Assert.IsTrue(Directory.Exists(clusterCachePath), "explicit cluster cache directory exists after saving database");

            var dbCachePath = loader.GetDatabaseCachePath(db.Name, "cluster");
            Assert.IsTrue(File.Exists(dbCachePath), "database cache file exists after saving database");

            var loadedDb = await loader.LoadDatabaseAsync("db", "cluster");
            Assert.IsNotNull(loadedDb);
            AssertEqual(db, loadedDb);

            loader.DeleteCache();
        }

        [TestMethod]
        public async Task TestFileSymbolLoader_SaveClusterAsyc()
        {
            var cachePath = GetTestCachePath();
            var loader = new FileSymbolLoader(cachePath, "default_cluster");

            var cluster = new ClusterSymbol("cluster", 
                new DatabaseSymbol("db", new TableSymbol("Table", "(x: long, y: string)")),
                new DatabaseSymbol("db2", new TableSymbol("Table2", "(a: stirng, b: datetime)")));
            
            await loader.SaveClusterAsync(cluster);

            var defaultClusterCachePath = loader.GetClusterCachePath("default_cluster");
            Assert.IsFalse(Directory.Exists(defaultClusterCachePath), "default cluster cache directory does not exists after saving cluster");

            var clusterCachePath = loader.GetClusterCachePath("cluster");
            Assert.IsTrue(Directory.Exists(clusterCachePath), "explicit cluster cache directory exists after saving cluster");

            var loadedDb = await loader.LoadDatabaseAsync("db", cluster.Name);
            Assert.IsNotNull(loadedDb);
            AssertEqual(cluster.GetDatabase("db"), loadedDb);

            var loadedDb2 = await loader.LoadDatabaseAsync("db2", cluster.Name);
            Assert.IsNotNull(loadedDb2);
            AssertEqual(cluster.GetDatabase("db2"), loadedDb2);

            loader.DeleteCache();
        }

        [TestMethod]
        public async Task TestFileSymbolLoader_DeleteCache()
        {
            var cachePath = GetTestCachePath();
            var loader = new FileSymbolLoader(cachePath, "cluster");

            Assert.IsFalse(Directory.Exists(cachePath), "cache does not exist before test");

            var db = new DatabaseSymbol("db", new TableSymbol("Table", "(x: long, y: string)"));
            await loader.SaveDatabaseAsync(db);

            Assert.IsTrue(Directory.Exists(cachePath), "cache exists after saving database");

            loader.DeleteCache();

            Assert.IsFalse(Directory.Exists(cachePath), "cache no longer exists after call to DeleteCache()");
        }

        [TestMethod]
        public async Task TestFileSymbolLoader_DeleteClusterCache_default_cluster()
        {
            var cachePath = GetTestCachePath();
            var loader = new FileSymbolLoader(cachePath, "default_cluster");

            Assert.IsFalse(Directory.Exists(cachePath), "cache does not exist before test");

            var db = new DatabaseSymbol("db", new TableSymbol("Table", "(x: long, y: string)"));
            await loader.SaveDatabaseAsync(db);

            var clusterCachePath = loader.GetClusterCachePath();
            Assert.IsTrue(Directory.Exists(clusterCachePath), "default cluster path exists after saving database");

            loader.DeleteClusterCache();

            Assert.IsFalse(Directory.Exists(clusterCachePath), "cluster cache no longer exists after call to DeleteClusterCache()");
            Assert.IsTrue(Directory.Exists(cachePath), "cache still exists after call to DeleteClusterCache()");

            loader.DeleteCache();

            Assert.IsFalse(Directory.Exists(cachePath), "cache no longer exists after call to DeleteCache()");
        }

        [TestMethod]
        public async Task TestFileSymbolLoader_DeleteClusterCache_explicit_cluster()
        {
            var cachePath = GetTestCachePath();
            var loader = new FileSymbolLoader(cachePath, "default_cluster");

            Assert.IsFalse(Directory.Exists(cachePath), "cache does not exist before test");

            var db = new DatabaseSymbol("db", new TableSymbol("Table", "(x: long, y: string)"));
            await loader.SaveDatabaseAsync(db, "cluster");

            var defaultClusterCachePath = loader.GetClusterCachePath();
            Assert.IsFalse(Directory.Exists(defaultClusterCachePath), "default cluster path does not exist after saving database");

            var clusterCachePath = loader.GetClusterCachePath("cluster");
            Assert.IsTrue(Directory.Exists(clusterCachePath), "cluster path exists after saving database");

            loader.DeleteClusterCache("cluster");

            Assert.IsFalse(Directory.Exists(clusterCachePath), "cluster cache no longer exists after call to DeleteClusterCache()");
            Assert.IsTrue(Directory.Exists(cachePath), "cache still exists after call to DeleteClusterCache()");

            loader.DeleteCache();

            Assert.IsFalse(Directory.Exists(cachePath), "cache no longer exists after call to DeleteCache()");
        }

        [TestMethod]
        public async Task TestCachedServerSymbolLoader_LoadDatabase()
        {
            var cachePath = GetTestCachePath();

            using (var loader = new CachedServerSymbolLoader(HelpConnection, cachePath))
            {
                var dbCachePath = loader.FileLoader.GetDatabaseCachePath("Samples");
                Assert.IsFalse(File.Exists(dbCachePath), "database cache does not exist before load request");

                var db = await loader.LoadDatabaseAsync("Samples");
                Assert.IsNotNull(db);

                Assert.IsTrue(File.Exists(dbCachePath), "database cache exists after load request");

                var cachedDb = await loader.FileLoader.LoadDatabaseAsync("Samples");
                Assert.IsNotNull(cachedDb);
                AssertEqual(db, cachedDb);

                loader.FileLoader.DeleteCache();
            }
        }

        private static string GetTestCachePath()
        {
            return Path.Combine(Environment.CurrentDirectory, "TestCache_" + System.Guid.NewGuid());
        }

        private static void AssertEqual(Symbol expected, Symbol actual)
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
                default:
                    Assert.Fail($"Unhandled symbol type: {expected.Kind}");
                    break;
            }
        }

        private static void AssertEqual(ClusterSymbol expected, ClusterSymbol actual)
        {
            Assert.AreEqual(expected.Name, actual.Name, "name");
            Assert.AreEqual(expected.Members.Count, actual.Members.Count, "cluster member count");

            for (int i = 0; i < expected.Members.Count; i++)
            {
                AssertEqual(expected.Members[i], actual.Members[i]);
            }
        }

        private static void AssertEqual(DatabaseSymbol expected, DatabaseSymbol actual)
        {
            Assert.AreEqual(expected.Name, actual.Name, "name");
            Assert.AreEqual(expected.Members.Count, actual.Members.Count, $"database {expected.Name} member count");

            for (int i = 0; i < expected.Tables.Count; i++)
            {
                AssertEqual(expected.Tables[i], actual.Tables[i]);
            }

            for (int i = 0; i < expected.ExternalTables.Count; i++)
            {
                AssertEqual(expected.ExternalTables[i], actual.ExternalTables[i]);
            }

            for (int i = 0; i < expected.MaterializedViews.Count; i++)
            {
                AssertEqual(expected.MaterializedViews[i], actual.MaterializedViews[i]);
            }

            for (int i = 0; i < expected.Functions.Count; i++)
            {
                AssertEqual(expected.Functions[i], actual.Functions[i]);
            }
        }

        private static void AssertEqual(TableSymbol expected, TableSymbol actual)
        {
            Assert.AreEqual(expected.Name, actual.Name, "name");

            var expectedSchema = SymbolFacts.GetSchema(expected);
            var actualSchema = SymbolFacts.GetSchema(actual);
            Assert.AreEqual(expectedSchema, actualSchema, $"table '{expected.Name}' schema");
        }

        private static void AssertEqual(ExternalTableSymbol expected, ExternalTableSymbol actual)
        {
            Assert.AreEqual(expected.Name, actual.Name, "name");

            var expectedSchema = SymbolFacts.GetSchema(expected);
            var actualSchema = SymbolFacts.GetSchema(actual);
            Assert.AreEqual(expectedSchema, actualSchema, $"external table '{expected.Name}' schema");
        }

        private static void AssertEqual(MaterializedViewSymbol expected, MaterializedViewSymbol actual)
        {
            Assert.AreEqual(expected.Name, actual.Name, "name");

            var expectedSchema = SymbolFacts.GetSchema(expected);
            var actualSchema = SymbolFacts.GetSchema(actual);
            Assert.AreEqual(expectedSchema, actualSchema, $"materialized view '{expected.Name}' schema");

            Assert.AreEqual(expected.MaterializedViewQuery, actual.MaterializedViewQuery, $"materialzed view '{expected.Name}' query");
        }

        private static void AssertEqual(FunctionSymbol expected, FunctionSymbol actual)
        {
            Assert.AreEqual(expected.Name, actual.Name, "name");

            var expectedParameters = SymbolFacts.GetParameterList(expected);
            var actualParameters = SymbolFacts.GetParameterList(actual);
            Assert.AreEqual(expectedParameters, actualParameters, $"function '{expected.Name}' parameters");

            var expectedBody = expected.Signatures[0].Body;
            var actualBody = actual.Signatures[0].Body;

            Assert.AreEqual(expectedBody, actualBody, $"function '{expected.Name}' body");
        }
    }
}
