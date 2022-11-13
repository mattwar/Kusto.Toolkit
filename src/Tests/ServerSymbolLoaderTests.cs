using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Kusto.Toolkit;

namespace Tests
{
    [TestClass]
    public class ServerSymbolLoaderTests : SymbolLoaderTestBase
    {
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
                Assert.IsTrue(names.Any(n => n.Name == "Samples"));
            }
        }
    }
}