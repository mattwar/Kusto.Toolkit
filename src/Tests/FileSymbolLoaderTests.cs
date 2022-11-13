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
    }
}