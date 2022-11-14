using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Kusto.Language;
using Kusto.Toolkit;
using Kusto.Language.Symbols;
using System.IO;

namespace Tests
{
    [TestClass]
    public class FileSymbolLoaderTests : SymbolLoaderTestBase
    {
        // Run this test to update the predefined test cache files in the test project Schema directory
#if false
        [TestMethod]
        public async Task UpdateTestSchemaFiles()
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
        public Task TestLoadDatabaseNamesAsync()
        {
            // the save test tests both load and save
            return TestSaveDatabaseNamesAsync();
        }

        [TestMethod]
        public async Task TestSaveDatabaseNamesAsync()
        {
            var cachePath = GetTestCachePath();
            var loader = new FileSymbolLoader(cachePath, "cluster");

            var cluster = new ClusterSymbol("cluster",
                new DatabaseSymbol("db", "pretty_db", new TableSymbol("Table", "(x: long, y: string)")),
                new DatabaseSymbol("db2", new TableSymbol("Table2", "(a: stirng, b: datetime)")));

            // before cluster exists, no names
            var namesBefore = await loader.LoadDatabaseNamesAsync("cluster");
            Assert.IsNull(namesBefore);

            // after cluster saved
            await loader.SaveClusterAsync(cluster);

            var namesAfter = await loader.LoadDatabaseNamesAsync("cluster");
            Assert.IsNotNull(namesAfter);
            Assert.AreEqual(2, namesAfter.Count);
            Assert.AreEqual("db", namesAfter[0].Name);
            Assert.AreEqual("pretty_db", namesAfter[0].PrettyName);
            Assert.AreEqual("db2", namesAfter[1].Name);
            Assert.AreEqual("", namesAfter[1].PrettyName);
        }

        [TestMethod]
        public async Task TestLoadDatabaseAsync_default_cluster()
        {
            await TestSaveDatabaseAsync_default_cluster();
        }

        [TestMethod]
        public async Task TestLoadDatabaseAsync_explicit_cluster()
        {
            await TestSaveDatabaseAsync_explicit_cluster();
        }

        [TestMethod]
        public async Task TestSaveDatabaseAsync_default_cluster()
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
        public async Task TestSaveDatabaseAsync_explicit_cluster()
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
        public async Task TestSaveClusterAsync()
        {
            var cachePath = GetTestCachePath();
            var loader = new FileSymbolLoader(cachePath, "default_cluster");

            var cluster = new ClusterSymbol("cluster",
                new DatabaseSymbol("db", new TableSymbol("Table", "(x: long, y: string)")),
                new DatabaseSymbol("db2", new TableSymbol("Table2", "(a: stirng, b: datetime)")));

            var defaultClusterCachePath = loader.GetClusterCachePath("default_cluster");
            var clusterCachePath = loader.GetClusterCachePath("cluster");
            var databaseNamesFilePath = loader.GetDatabaseNamesPath("cluster");

            Assert.IsFalse(Directory.Exists(defaultClusterCachePath), "default cluster cache directory does not exists after saving cluster");
            Assert.IsFalse(Directory.Exists(clusterCachePath), "explicit cluster cache directory exists after saving cluster");
            Assert.IsFalse(File.Exists(databaseNamesFilePath), "database names file exists after saving cache");

            await loader.SaveClusterAsync(cluster);

            Assert.IsFalse(Directory.Exists(defaultClusterCachePath), "default cluster cache directory does not exists after saving cluster");
            Assert.IsTrue(Directory.Exists(clusterCachePath), "explicit cluster cache directory exists after saving cluster");
            Assert.IsTrue(File.Exists(databaseNamesFilePath), "database names file exists after saving cache");

            var loadedDb = await loader.LoadDatabaseAsync("db", cluster.Name);
            Assert.IsNotNull(loadedDb);
            AssertEqual(cluster.GetDatabase("db"), loadedDb);

            var loadedDb2 = await loader.LoadDatabaseAsync("db2", cluster.Name);
            Assert.IsNotNull(loadedDb2);
            AssertEqual(cluster.GetDatabase("db2"), loadedDb2);

            loader.DeleteCache();
        }

        [TestMethod]
        public async Task TestSaveClustersAsync()
        {
            var cachePath = GetTestCachePath();
            var loader = new FileSymbolLoader(cachePath, "default_cluster");

            var clusters = new ClusterSymbol[]
            {
                new ClusterSymbol("cluster1",
                    new DatabaseSymbol("db11", new TableSymbol("Table", "(x: long, y: string)"))),

                new ClusterSymbol("cluster2",
                    new DatabaseSymbol("db21", new TableSymbol("Table", "(x: long, y: string)")),
                    new DatabaseSymbol("db22", new TableSymbol("Table2", "(a: stirng, b: datetime)")))
            };

            var cluster1CachePath = loader.GetClusterCachePath("cluster1");
            var cluster2CachePath = loader.GetClusterCachePath("cluster1");

            Assert.IsFalse(Directory.Exists(cluster1CachePath));
            Assert.IsFalse(Directory.Exists(cluster2CachePath));

            await loader.SaveClustersAsync(clusters);

            Assert.IsTrue(Directory.Exists(cluster1CachePath));
            Assert.IsTrue(Directory.Exists(cluster2CachePath));

            var cluster1DbNames = await loader.LoadDatabaseNamesAsync("cluster1");
            Assert.IsNotNull(cluster1DbNames);
            Assert.AreEqual(1, cluster1DbNames.Count);

            var cluster2DbNames = await loader.LoadDatabaseNamesAsync("cluster2");
            Assert.IsNotNull(cluster2DbNames);
            Assert.AreEqual(2, cluster2DbNames.Count);

            loader.DeleteCache();
        }

        [TestMethod]
        public async Task TestDeleteCache()
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
        public async Task TestDeleteClusterCache_default_cluster()
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
        public async Task TestDeleteClusterCache_explicit_cluster()
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
        public void TestGetClusterCachePath_implicit_cluster()
        {
            var cachePath = GetTestCachePath();
            var loader = new FileSymbolLoader(cachePath, "cluster");
            var clusterCachePath = loader.GetClusterCachePath();
            Assert.IsTrue(clusterCachePath.EndsWith("cluster.kusto.windows.net"));
            loader.DeleteCache();
        }

        [TestMethod]
        public void TestGetClusterCachePath_explicit_cluster()
        {
            var cachePath = GetTestCachePath();
            var loader = new FileSymbolLoader(cachePath, "some_other_cluster");
            var clusterCachePath = loader.GetClusterCachePath("cluster");
            Assert.IsTrue(clusterCachePath.EndsWith("cluster.kusto.windows.net"));
            loader.DeleteCache();
        }

        [TestMethod]
        public void TestGetDatabaseCachePath_implicit_cluster()
        {
            var cachePath = GetTestCachePath();
            var loader = new FileSymbolLoader(cachePath, "cluster");
            var dbCachePath = loader.GetDatabaseCachePath("db");
            Assert.IsTrue(dbCachePath.EndsWith(Path.Combine("cluster.kusto.windows.net", "db.json")));
            loader.DeleteCache();
        }

        [TestMethod]
        public void TestGetDatabaseCachePath_explicit_cluster()
        {
            var cachePath = GetTestCachePath();
            var loader = new FileSymbolLoader(cachePath, "some_other_cluster");
            var dbCachePath = loader.GetDatabaseCachePath("db", "cluster");
            Assert.IsTrue(dbCachePath.EndsWith(Path.Combine("cluster.kusto.windows.net", "db.json")));
            loader.DeleteCache();
        }

        [TestMethod]
        public void TestGetDatabaseNamesPath_implicit_cluster()
        {
            var cachePath = GetTestCachePath();
            var loader = new FileSymbolLoader(cachePath, "cluster");
            var dbNamesPath = loader.GetDatabaseNamesPath();
            Assert.IsTrue(dbNamesPath.EndsWith(Path.Combine("cluster.kusto.windows.net", "databaseNames.json")));
            loader.DeleteCache();
        }

        [TestMethod]
        public void TestGetDatabaseNamesPath_explicit_cluster()
        {
            var cachePath = GetTestCachePath();
            var loader = new FileSymbolLoader(cachePath, "some_other_cluster");
            var dbNamesPath = loader.GetDatabaseNamesPath("cluster");
            Assert.IsTrue(dbNamesPath.EndsWith(Path.Combine("cluster.kusto.windows.net", "databaseNames.json")));
            loader.DeleteCache();
        }
    }
}