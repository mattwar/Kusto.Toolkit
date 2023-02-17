using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Kusto.Toolkit;
using Kusto.Data;

namespace Tests
{
    [TestClass]
    public class CachedSymbolLoaderTests : SymbolLoaderTestBase
    {
        private CachedSymbolLoader CreateLoader(string cachePath = null)
        {
            cachePath ??= GetTestCachePath();
            return new CachedSymbolLoader(new KustoConnectionStringBuilder(HelpConnection), cachePath);
        }

        [TestMethod]
        public async Task TestLoadDatabaseNamesAsync_implicit_cluster()
        {
            using (var loader = CreateLoader())
            {
                var dbNamesFilePath = loader.FileLoader.GetDatabaseNamesPath();
                Assert.IsFalse(File.Exists(dbNamesFilePath));

                var dbNames = await loader.LoadDatabaseNamesAsync();
                Assert.IsNotNull(dbNames);
                Assert.IsTrue(File.Exists(dbNamesFilePath));

                loader.FileLoader.DeleteCache();
            }
        }

        [TestMethod]
        public async Task TestLoadDatabaseNamesAsync_explicit_cluster()
        {
            using (var loader = CreateLoader())
            {
                var dbNamesFilePath = loader.FileLoader.GetDatabaseNamesPath();
                Assert.IsFalse(File.Exists(dbNamesFilePath));

                var dbNames = await loader.LoadDatabaseNamesAsync(HelpCluster);
                Assert.IsNotNull(dbNames);
                Assert.IsTrue(File.Exists(dbNamesFilePath));

                loader.FileLoader.DeleteCache();
            }
        }

        [TestMethod]
        public async Task TestLoadDatabaseNamesAsync_unknown_cluster()
        {
            using (var loader = CreateLoader())
            {
                var dbNamesFilePath = loader.FileLoader.GetDatabaseNamesPath("unknown_cluster");
                Assert.IsFalse(File.Exists(dbNamesFilePath));

                var dbNames = await loader.LoadDatabaseNamesAsync("unknown_cluster");
                Assert.IsNull(dbNames);

                loader.FileLoader.DeleteCache();
            }
        }

        [TestMethod]
        public async Task TestLoadDatabaseAsync_implicit_cluster()
        {
            using (var loader = CreateLoader())
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

        [TestMethod]
        public async Task TestLoadDatabaseAsync_explicit_cluster()
        {
            using (var loader = CreateLoader())
            {
                var dbCachePath = loader.FileLoader.GetDatabaseCachePath("Samples");
                Assert.IsFalse(File.Exists(dbCachePath), "database cache does not exist before load request");

                var db = await loader.LoadDatabaseAsync("Samples", HelpCluster);
                Assert.IsNotNull(db);

                Assert.IsTrue(File.Exists(dbCachePath), "database cache exists after load request");

                var cachedDb = await loader.FileLoader.LoadDatabaseAsync("Samples");
                Assert.IsNotNull(cachedDb);
                AssertEqual(db, cachedDb);

                loader.FileLoader.DeleteCache();
            }
        }

        [TestMethod]
        public async Task TestLoadDatabaseAsync_unknown_cluster()
        {
            var cachePath = GetTestCachePath();

            using (var loader = CreateLoader())
            {
                var dbCachePath = loader.FileLoader.GetDatabaseCachePath("Samples", "unknown_cluster");
                Assert.IsFalse(File.Exists(dbCachePath));

                var db = await loader.LoadDatabaseAsync("Samples", "unknown_cluster");
                Assert.IsNull(db);

                Assert.IsFalse(File.Exists(dbCachePath));

                var cachedDb = await loader.FileLoader.LoadDatabaseAsync("Samples");
                Assert.IsNull(cachedDb);

                loader.FileLoader.DeleteCache();
            }
        }
    }
}