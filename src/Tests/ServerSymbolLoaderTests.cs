using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Kusto.Data;
using Kusto.Language;
using Kusto.Language.Symbols;
using Kusto.Toolkit;
using System.IO;

namespace Tests
{
    [TestClass]
    public class ServerSymbolLoaderTests : SymbolLoaderTestBase
    {
        private ServerSymbolLoader CreateLoader(string connection = null)
        {
            return new ServerSymbolLoader(new KustoConnectionStringBuilder(connection ?? HelpConnection));
        }

        [TestMethod]
        public async Task TestLoadDatabaseNamesAsync_implicit_cluster()
        {
            using (var loader = CreateLoader())
            {
                var names = await loader.LoadDatabaseNamesAsync();
                Assert.IsNotNull(names);
                Assert.IsTrue(names.Any(n => n.Name == "Samples"));
            }
        }

        [TestMethod]
        public async Task TestLoadDatabaseNamesAsync_explicit_cluster_short_name()
        {
            using (var loader = CreateLoader())
            {
                var names = await loader.LoadDatabaseNamesAsync("help");
                Assert.IsNotNull(names);
                Assert.IsTrue(names.Any(n => n.Name == "Samples"));
            }
        }

        [TestMethod]
        public async Task TestLoadDatabaseNamesAsync_explicit_cluster_full_name()
        {
            using (var loader = CreateLoader())
            {
                var names = await loader.LoadDatabaseNamesAsync(HelpCluster);
                Assert.IsNotNull(names);
                Assert.IsTrue(names.Any(n => n.Name == "Samples"));
            }
        }

        [TestMethod]
        public async Task TestLoadDatabaseNamesAsync_explicit_cluster_wrong_case()
        {
            using (var loader = CreateLoader())
            {
                var names = await loader.LoadDatabaseNamesAsync(HelpCluster.ToUpper());
                Assert.IsNotNull(names);
                Assert.IsTrue(names.Any(n => n.Name == "Samples"));
            }
        }

        [TestMethod]
        public async Task TestLoadDatabaseNamesAsync_unknown_cluster()
        {
            using (var loader = CreateLoader())
            {
                var names = await loader.LoadDatabaseNamesAsync("unknown_cluster");
                Assert.IsNull(names);
            }
        }

        [TestMethod]
        public async Task TestLoadDatabaseAsync_implicit_cluster()
        {
            using (var loader = CreateLoader())
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
        public async Task TestLoadDatabaseAsync_explicit_cluster()
        {
            using (var loader = CreateLoader())
            {
                var dbSymbol = await loader.LoadDatabaseAsync("Samples", HelpCluster);
                Assert.IsNotNull(dbSymbol);

                Assert.IsTrue(dbSymbol.Members.Count > 0, "members count");
                Assert.IsTrue(dbSymbol.Tables.Count > 0, "tables count");
                Assert.IsTrue(dbSymbol.MaterializedViews.Count > 0, "materialized views count");
                Assert.IsTrue(dbSymbol.Functions.Count > 0, "functions count");
            }
        }

        [TestMethod]
        public async Task TestLoadSamples_versus_baseline()
        {
            using (var loader = CreateLoader())
            {
                var dbSymbol = await loader.LoadDatabaseAsync("Samples", HelpCluster);
                Assert.IsNotNull(dbSymbol);

                var cachePath = Path.Combine(Environment.CurrentDirectory, "Schema");
                var baseLineLoader = new FileSymbolLoader(cachePath, HelpCluster);
                var dbBaseline = await baseLineLoader.LoadDatabaseAsync(dbSymbol.Name, throwOnError: true);
                Assert.IsNotNull(dbBaseline);

                var possibleNewBaseline = FileSymbolLoader.SerializeDatabase(dbSymbol);

                AssertEqual(dbBaseline, dbSymbol);
            }
        }

        [TestMethod]
        public async Task TestLoadDatabaseAsync_unknown_cluster()
        {
            using (var loader = CreateLoader())
            {
                var dbSymbol = await loader.LoadDatabaseAsync("Samples", "unknown_cluster");
                Assert.IsNull(dbSymbol);
            }
        }

        [TestMethod]
        public async Task TestLoadDatabaseAsync_unknown_database()
        {
            using (var loader = CreateLoader())
            {
                var dbSymbol = await loader.LoadDatabaseAsync("not-a-db");
                Assert.IsNull(dbSymbol);
            }
        }

#if false
        [TestMethod]
        public async Task TestLoadDatabaseAsync_PrettyName()       
        {
            // test with a private cluster since no databases in Help cluster have pretty names
            using (var loader = CreateLoader("Data Source=https://kvcc4b8b0165e074002bc5.southcentralus.kusto.windows.net;Fed=true"))
            {
                var dbSymbol = await loader.LoadDatabaseAsync("PrettyZomg");
                Assert.IsNotNull(dbSymbol);
            }
        }
#endif
    }
}