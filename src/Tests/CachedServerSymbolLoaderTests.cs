using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Kusto.Toolkit;

namespace Tests
{
    [TestClass]
    public class CachedServerSymbolLoaderTests : SymbolLoaderTestBase
    {
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
    }
}