using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Kusto.Language;
using Kusto.Toolkit;
using Kusto.Language.Symbols;

namespace Tests
{
    [TestClass]
    public class SymbolLoaderTests : SymbolLoaderTestBase
    {
        [TestMethod]
        public async Task TestAddOrUpdateDatabaseAsync()
        {
            var globals = GlobalState.Default;
            var loader = new TestLoader(OneTestCluster);

            var newGlobals = await loader.AddOrUpdateDatabaseAsync(globals, "db1");

            // a new cluster is added with the loaded Samples database
            Assert.AreEqual(1, newGlobals.Clusters.Count);
            var cluster1 = newGlobals.Clusters[0];
            Assert.AreEqual("cluster1.kusto.windows.net", cluster1.Name);
            Assert.AreEqual(1, cluster1.Databases.Count);
            var db1 = cluster1.Databases[0];
            Assert.AreEqual("db1", db1.Name);
            Assert.AreNotEqual(0, db1.Members.Count);

            // default cluster and database are not set
            Assert.AreNotSame(newGlobals.Cluster, cluster1);
            Assert.AreNotSame(newGlobals.Database, db1);
        }

        [TestMethod]
        public async Task TestAddOrUpdateDefaultDatabaseAsync()
        {
            var globals = GlobalState.Default;
            var loader = new TestLoader(OneTestCluster);

            var newGlobals = await loader.AddOrUpdateDefaultDatabaseAsync(globals, "db1");

            // a new cluster is added with the loaded Samples database
            Assert.AreEqual(1, newGlobals.Clusters.Count);
            var cluster1 = newGlobals.Clusters[0];
            Assert.AreEqual("cluster1.kusto.windows.net", cluster1.Name);
            Assert.AreEqual(1, cluster1.Databases.Count);
            var db1 = cluster1.Databases[0];
            Assert.AreEqual("db1", db1.Name);
            Assert.AreNotEqual(0, db1.Members.Count);

            // default cluster and database are set
            Assert.AreSame(newGlobals.Cluster, cluster1);
            Assert.AreSame(newGlobals.Database, db1);
        }

        [TestMethod]
        public async Task TestAddDatabaseAsync()
        {
            var globals = GlobalState.Default;
            var loader = new TestLoader(OneTestCluster);

            var newGlobals = await loader.AddDatabaseAsync(globals, "db1");

            // a new cluster is added with the loaded Samples database
            Assert.AreEqual(1, newGlobals.Clusters.Count);
            var cluster1 = newGlobals.Clusters[0];
            Assert.AreEqual("cluster1.kusto.windows.net", cluster1.Name);
            Assert.AreEqual(1, cluster1.Databases.Count);
            var db1 = cluster1.Databases[0];
            Assert.AreEqual("db1", db1.Name);
            Assert.AreNotEqual(0, db1.Members.Count);

            // default cluster and database are not set
            Assert.AreNotSame(newGlobals.Cluster, cluster1);
            Assert.AreNotSame(newGlobals.Database, db1);

            // called again does nothing since it is already defined
            var newGlobals2 = await loader.AddDatabaseAsync(newGlobals, "db1");
            Assert.AreSame(newGlobals2, newGlobals2);
        }

        [TestMethod]
        public async Task TestAddDefaultDatabaseAsync()
        {
            var globals = GlobalState.Default;
            var loader = new TestLoader(OneTestCluster);

            var newGlobals = await loader.AddDefaultDatabaseAsync(globals, "db1");

            // a new cluster is added with the loaded Samples database
            Assert.AreEqual(1, newGlobals.Clusters.Count);
            var cluster1 = newGlobals.Clusters[0];
            Assert.AreEqual("cluster1.kusto.windows.net", cluster1.Name);
            Assert.AreEqual(1, cluster1.Databases.Count);
            var db1 = cluster1.Databases[0];
            Assert.AreEqual("db1", db1.Name);
            Assert.AreNotEqual(0, db1.Members.Count);

            // default cluster and database are set
            Assert.AreSame(newGlobals.Cluster, cluster1);
            Assert.AreSame(newGlobals.Database, db1);

            // called again does nothing since it is already defined
            var newGlobals2 = await loader.AddDefaultDatabaseAsync(newGlobals, "db1");
            Assert.AreSame(newGlobals, newGlobals2);
        }

        [TestMethod]
        public async Task TestAddDefaultDatabaseAsync_definedButNotDefault()
        {
            var globals = GlobalState.Default;
            var loader = new TestLoader(OneTestCluster);

            var newGlobals = await loader.AddDatabaseAsync(globals, "db1");

            // a new cluster is added with the loaded Samples database
            Assert.AreEqual(1, newGlobals.Clusters.Count);
            var cluster1 = newGlobals.Clusters[0];
            Assert.AreEqual("cluster1.kusto.windows.net", cluster1.Name);
            Assert.AreEqual(1, cluster1.Databases.Count);
            var db1 = cluster1.Databases[0];
            Assert.AreEqual("db1", db1.Name);
            Assert.AreNotEqual(0, db1.Members.Count);

            // default cluster and database are not set
            Assert.AreNotSame(newGlobals.Cluster, cluster1);
            Assert.AreNotSame(newGlobals.Database, db1);

            // called again does not reload, but does assign default
            var newGlobals2 = await loader.AddDefaultDatabaseAsync(newGlobals, "db1");
            Assert.AreNotSame(newGlobals, newGlobals2);

            // default cluster and database are now set
            Assert.AreSame(newGlobals2.Cluster, cluster1);
            Assert.AreSame(newGlobals2.Database, db1);
        }

        [TestMethod]
        public async Task TestAddOrUpdateClusterAsync()
        {
            var loader = new TestLoader(OneTestCluster);

            // no clusters, but default cluster and database are set with no names.
            var globals = GlobalState.Default;

            var newGlobals = await loader.AddOrUpdateClusterAsync(globals);
            Assert.AreNotSame(globals, newGlobals);
            Assert.AreNotEqual(0, newGlobals.Clusters.Count);
            var cluster1 = newGlobals.GetCluster("cluster1");
            Assert.IsNotNull(cluster1);
            Assert.AreEqual(2, cluster1.Databases.Count);
            Assert.IsTrue(cluster1.Databases[0].IsOpen);
            Assert.IsTrue(cluster1.Databases[1].IsOpen);

            // was not made default
            Assert.AreNotSame(newGlobals.Cluster, cluster1);
        }

        [TestMethod]
        public async Task TestAddOrUpdateDefaultClusterAsync()
        {
            var loader = new TestLoader(OneTestCluster, "cluster1");

            // no clusters, but default cluster and database are set with no names.
            var globals = GlobalState.Default;

            var newGlobals = await loader.AddOrUpdateDefaultClusterAsync(globals);
            Assert.AreNotSame(globals, newGlobals);
            Assert.AreNotEqual(0, newGlobals.Clusters.Count);
            var cluster1 = newGlobals.GetCluster("cluster1");
            Assert.IsNotNull(cluster1);
            Assert.AreEqual(2, cluster1.Databases.Count);
            Assert.IsTrue(cluster1.Databases[0].IsOpen);
            Assert.IsTrue(cluster1.Databases[1].IsOpen);

            // was made default
            Assert.AreSame(newGlobals.Cluster, cluster1);
        }

        [TestMethod]
        public async Task TestAddClusterAsync()
        {
            var loader = new TestLoader(OneTestCluster);

            // no clusters, but default cluster and database are set with no names.
            var globals = GlobalState.Default;

            var newGlobals = await loader.AddClusterAsync(globals);
            Assert.AreNotSame(globals, newGlobals);
            Assert.AreNotEqual(0, newGlobals.Clusters.Count);
            var cluster1 = newGlobals.GetCluster("cluster1");
            Assert.IsNotNull(cluster1);
            Assert.AreEqual(2, cluster1.Databases.Count);
            Assert.IsTrue(cluster1.Databases[0].IsOpen);
            Assert.IsTrue(cluster1.Databases[1].IsOpen);

            // was not made default
            Assert.AreNotSame(newGlobals.Cluster, cluster1);

            // is not loaded again
            var newGlobals2 = await loader.AddClusterAsync(newGlobals);
            Assert.AreSame(newGlobals, newGlobals2);
        }

        [TestMethod]
        public async Task TestAddDefaultClusterAsync()
        {
            var loader = new TestLoader(OneTestCluster, "cluster1");

            // no clusters, but default cluster and database are set with no names.
            var globals = GlobalState.Default;

            var newGlobals = await loader.AddDefaultClusterAsync(globals);
            Assert.AreNotSame(globals, newGlobals);
            Assert.AreNotEqual(0, newGlobals.Clusters.Count);
            var cluster1 = newGlobals.GetCluster("cluster1");
            Assert.IsNotNull(cluster1);
            Assert.AreEqual(2, cluster1.Databases.Count);
            Assert.IsTrue(cluster1.Databases[0].IsOpen);
            Assert.IsTrue(cluster1.Databases[1].IsOpen);

            // was made default
            Assert.AreSame(newGlobals.Cluster, cluster1);

            // is not loaded again
            var newGlobals2 = await loader.AddClusterAsync(newGlobals);
            Assert.AreSame(newGlobals, newGlobals2);
        }

        [TestMethod]
        public async Task TestAddDefaultClusterAsync_definedButNotDefault()
        {
            var loader = new TestLoader(OneTestCluster);

            // no clusters, but default cluster and database are set with no names.
            var globals = GlobalState.Default;

            var newGlobals = await loader.AddClusterAsync(globals);
            Assert.AreNotSame(globals, newGlobals);
            Assert.AreNotEqual(0, newGlobals.Clusters.Count);
            var cluster1 = newGlobals.GetCluster("cluster1");
            Assert.IsNotNull(cluster1);
            Assert.AreEqual(2, cluster1.Databases.Count);
            Assert.IsTrue(cluster1.Databases[0].IsOpen);
            Assert.IsTrue(cluster1.Databases[1].IsOpen);

            // was not made default
            Assert.AreNotSame(newGlobals.Cluster, cluster1);

            // is not reloaded but is made default
            var newGlobals2 = await loader.AddDefaultClusterAsync(newGlobals);
            Assert.AreNotSame(newGlobals, newGlobals2);
            Assert.AreSame(newGlobals2.Cluster, cluster1);
        }
    }
}