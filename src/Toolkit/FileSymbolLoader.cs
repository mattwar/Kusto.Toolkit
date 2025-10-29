using System.Text;
using Kusto.Language;
using Kusto.Language.Symbols;
using Newtonsoft.Json;

#nullable disable // some day...

namespace Kusto.Toolkit
{
    using static SymbolFacts;

    /// <summary>
    /// A <see cref="SymbolLoader"/> that loads symbol schema from text files.
    /// </summary>
    public class FileSymbolLoader : SymbolLoader
    {
        private readonly string _schemaDirectoryPath;
        private readonly string _defaultClusterName;
        private readonly string _defaultDomain;

        public FileSymbolLoader(string schemaDirectoryPath, string defaultClusterName, string defaultDomain = null)
        {
            if (schemaDirectoryPath == null)
                throw new ArgumentNullException(nameof(schemaDirectoryPath));

            if (defaultClusterName == null)
                throw new ArgumentNullException(nameof(defaultClusterName));

            if (string.IsNullOrEmpty(schemaDirectoryPath))
                throw new ArgumentNullException("Invalid schema directory path", nameof(schemaDirectoryPath));

            if (string.IsNullOrWhiteSpace(defaultClusterName))
                throw new ArgumentNullException("Invalid default cluster name", nameof(defaultClusterName));

            _schemaDirectoryPath = Environment.ExpandEnvironmentVariables(schemaDirectoryPath);
            _defaultDomain = defaultDomain ?? KustoFacts.KustoWindowsNet;
            _defaultClusterName = GetFullHostName(defaultClusterName, _defaultDomain);
        }

        public override string DefaultCluster => _defaultClusterName;
        public override string DefaultDomain => _defaultDomain;

        /// <summary>
        /// Loads a list of database names for the specified cluster.
        /// If the cluster name is not specified, the loader's default cluster name is used.
        /// Returns null if the cluster is not found.
        /// </summary>
        public override async Task<IReadOnlyList<DatabaseName>> LoadDatabaseNamesAsync(string clusterName = null, CancellationToken cancellationToken = default)
        {
            var dbNamesPath = GetDatabaseNamesPath(clusterName);
            if (dbNamesPath != null && File.Exists(dbNamesPath))
            {
                var jsonText = await ReadAllTextAsync(dbNamesPath, cancellationToken).ConfigureAwait(false);
                var dbNames = JsonConvert.DeserializeObject<DatabaseNameInfo[]>(jsonText);
                if (dbNames != null)
                {
                    return dbNames.Select(info => new DatabaseName(info.Name, info.PrettyName)).ToArray();
                }
            }

            return null;
        }

        /// <summary>
        /// Saves the list of database names in the cache for the specified cluster.
        /// If the cluster name is not specified, the loader's default cluster name is used.
        /// Returns true if successful or false if not.
        /// </summary>
        public async Task SaveDatabaseNamesAsync(IReadOnlyList<DatabaseName> databaseNames, string clusterName = null, CancellationToken cancellationToken = default)
        {
            if (databaseNames == null)
                throw new ArgumentNullException(nameof(databaseNames));

            var dbNamesPath = GetDatabaseNamesPath(clusterName);
            var parentDir = Path.GetDirectoryName(dbNamesPath);

            if (!Directory.Exists(parentDir))
            {
                Directory.CreateDirectory(parentDir);
            }

            var dbNameInfos = databaseNames.Select(dn => new DatabaseNameInfo { Name = dn.Name, PrettyName = dn.PrettyName }).ToArray();
            var jsonText = JsonConvert.SerializeObject(dbNameInfos, s_serializationSettings);
            await WriteAllTextAsync(dbNamesPath, jsonText, cancellationToken).ConfigureAwait(false);
        }

        /// <summary>
        /// Adds the database name to the list of saved database names for the specified cluster.
        /// Returns true if successful or false if not.
        /// </summary>
        private async Task SaveDatabaseNameAsync(DatabaseName databaseName, string clusterName = null, CancellationToken cancellationToken = default)
        {
            if (databaseName == null)
                throw new ArgumentNullException(nameof(databaseName));

            if (String.IsNullOrEmpty(databaseName.Name))
                return;

            var dbNames = await LoadDatabaseNamesAsync(clusterName, cancellationToken).ConfigureAwait(false);
                
            bool nameAdded = false;
            if (dbNames != null)
            {
                var set = new SortedSet<DatabaseName>(dbNames, DatabaseNameComparer.Instance);
                nameAdded = set.Add(databaseName);
                dbNames = set.ToArray();
            }
            else
            {
                dbNames = new[] { databaseName };
                nameAdded = true;
            }

            if (nameAdded)
            {
                await SaveDatabaseNamesAsync(dbNames, clusterName, cancellationToken).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Loads the corresponding database's schema and returns a new <see cref="DatabaseSymbol"/> initialized from it.
        /// If the cluster name is not specified, the loader's default cluster name is used.
        /// </summary>
        public override async Task<DatabaseSymbol> LoadDatabaseAsync(string databaseName, string clusterName = null, CancellationToken cancellationToken = default)
        {
            if (databaseName == null)
                throw new ArgumentNullException(nameof(databaseName));

            if (string.IsNullOrWhiteSpace(databaseName))
                throw new ArgumentException("Invalid database name", nameof(databaseName));

            var databasePath = GetDatabaseCachePath(databaseName, clusterName);

            if (databasePath != null)
            {
                if (File.Exists(databasePath))
                {
                    var jsonText = await ReadAllTextAsync(databasePath, cancellationToken).ConfigureAwait(false);
                    var dbInfo = JsonConvert.DeserializeObject<DatabaseInfo>(jsonText);
                    return CreateDatabaseSymbol(dbInfo);
                }
            }

            return null;
        }

        /// <summary>
        /// Saves the database schema to the cache for the specified cluster.
        /// If the cluster name is not specified, the loader's default cluster name is used.
        /// </summary>
        public async Task SaveDatabaseAsync(DatabaseSymbol database, string clusterName = null, CancellationToken cancellationToken = default)
        {
            if (database == null)
                throw new ArgumentNullException(nameof(database));

            var clusterPath = GetClusterCachePath(clusterName);
            var databasePath = GetDatabaseCachePath(database.Name, clusterName);

            if (clusterPath != null && databasePath != null)
            {
                var jsonText = SerializeDatabase(database);

                if (!Directory.Exists(_schemaDirectoryPath))
                {
                    Directory.CreateDirectory(_schemaDirectoryPath);
                }

                if (!Directory.Exists(clusterPath))
                {
                    Directory.CreateDirectory(clusterPath);
                }

                await WriteAllTextAsync(databasePath, jsonText, cancellationToken).ConfigureAwait(false);
                var dbName = new DatabaseName(database.Name, database.AlternateName);
                await SaveDatabaseNameAsync(dbName, clusterName, cancellationToken).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Saves all the database schemas for the cluster to the cache.
        /// </summary>
        public async Task SaveClusterAsync(ClusterSymbol cluster, CancellationToken cancellationToken = default)
        {
            if (cluster == null)
                throw new ArgumentNullException(nameof(cluster));

            foreach (var db in cluster.Databases)
            {
                await SaveDatabaseAsync(db, cluster.Name, cancellationToken: cancellationToken).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Returns all the text read from the file asynchronously.
        /// </summary>
        private static Task<string> ReadAllTextAsync(string path, CancellationToken cancellationToken)
        {
#if NET6_0_OR_GREATER
            return File.ReadAllTextAsync(path, cancellationToken);
#else
            return Task.Run(() => File.ReadAllText(path), cancellationToken);
#endif
        }

        /// <summary>
        /// Creates and writes the text to the file asynchronously.
        /// </summary>
        private static Task WriteAllTextAsync(string path, string text, CancellationToken cancellationToken)
        {
#if NET6_0_OR_GREATER
            return File.WriteAllTextAsync(path, text, cancellationToken);
#else
            return Task.Run(() => File.WriteAllText(path, text), cancellationToken);
#endif
        }

        /// <summary>
        /// Saves all the database schemas for all the clusters to the cache.
        /// </summary>
        public async Task SaveClustersAsync(IEnumerable<ClusterSymbol> clusters, CancellationToken cancellationToken = default)
        {
            if (clusters == null)
                throw new ArgumentNullException(nameof(clusters));

            foreach (var cluster in clusters)
            {
                await SaveClusterAsync(cluster, cancellationToken).ConfigureAwait(false);
            }
        }

        private static readonly JsonSerializerSettings s_serializationSettings =
            new JsonSerializerSettings()
            {
                DefaultValueHandling = DefaultValueHandling.Ignore,
                Formatting = Formatting.Indented,
            };

        /// <summary>
        /// Gets the serialized representation of the <see cref="DatabaseSymbol"/>
        /// </summary>
        public static string SerializeDatabase(DatabaseSymbol database)
        {
            if (database == null)
                throw new ArgumentNullException(nameof(database));

            var dbInfo = CreateDatabaseInfo(database);
            return JsonConvert.SerializeObject(dbInfo, s_serializationSettings);
        }

        /// <summary>
        /// Gets the <see cref="DatabaseSymbol"/> for the serialization text of a database.
        /// </summary>
        public static DatabaseSymbol DeserializeDatabase(string serializedDatabase)
        {
            if (serializedDatabase == null)
                throw new ArgumentException(nameof(serializedDatabase));

            var dbInfo = JsonConvert.DeserializeObject<DatabaseInfo>(serializedDatabase);
            return CreateDatabaseSymbol(dbInfo);
        }

        /// <summary>
        /// Deletes all cached schemas for all clusters/databases
        /// </summary>
        public bool DeleteCache()
        {
            if (Directory.Exists(_schemaDirectoryPath))
            {
                try
                {
                    Directory.Delete(_schemaDirectoryPath, true);
                    return true;
                }
                catch (Exception)
                {
                }

                return false;
            }

            return true;
        }

        /// <summary>
        /// Deletes all cached schemas for all databases in the cluster.
        /// If the cluster name is not specified, the loader's default cluster name is used.
        /// </summary>
        public bool DeleteClusterCache(string clusterName = null)
        {
            var clusterPath = GetClusterCachePath(clusterName);
            if (clusterPath != null)
            {
                if (Directory.Exists(clusterPath))
                {
                    try
                    {
                        Directory.Delete(clusterPath, true);
                        return true;
                    }
                    catch (Exception)
                    {
                    }

                    return false;
                }
            }

            return true;
        }

        private class DatabaseNameComparer : IComparer<DatabaseName>
        {
            private DatabaseNameComparer() { }

            public int Compare(DatabaseName x, DatabaseName y)
            {
                return x.Name.CompareTo(y.Name);
            }

            public static readonly DatabaseNameComparer Instance = new DatabaseNameComparer();
        }

        /// <summary>
        /// Gets the path to the database names file.
        /// If the cluster name is not specified, the loader's default cluster name is used.
        /// </summary>
        public string GetDatabaseNamesPath(string clusterName = null)
        {
            clusterName = string.IsNullOrEmpty(clusterName)
                ? _defaultClusterName
                : GetFullHostName(clusterName, _defaultDomain);

            var clusterPath = GetClusterCachePath(clusterName);
            return Path.Combine(clusterPath, "databaseNames.json");
        }

        /// <summary>
        /// Gets path to the cluster cache directory within the schema cache directory.
        /// If the cluster name is not specified, the loader's default cluster name is used.
        /// </summary>
        public string GetClusterCachePath(string clusterName = null)
        {
            clusterName = string.IsNullOrEmpty(clusterName)
                ? _defaultClusterName
                : GetFullHostName(clusterName, _defaultDomain);

            var fullClusterName = GetFullHostName(clusterName, _defaultDomain);
            return Path.Combine(_schemaDirectoryPath, MakeFilePathPart(fullClusterName));
        }

        /// <summary>
        /// Gets the path to the database schema file.
        /// If the cluster name is not specified, the loader's default cluster name is used.
        /// </summary>
        public string GetDatabaseCachePath(string databaseName, string clusterName = null)
        {
            if (databaseName == null)
                throw new ArgumentNullException(nameof(databaseName));

            clusterName = string.IsNullOrEmpty(clusterName)
                ? _defaultClusterName
                : GetFullHostName(clusterName, _defaultDomain);

            return Path.Combine(GetClusterCachePath(clusterName), MakeFilePathPart(databaseName) + ".json");
        }

        private static readonly HashSet<char> _invalidPathChars =
            new HashSet<char>(Path.GetInvalidPathChars().Concat(Path.GetInvalidFileNameChars()));

        private static bool IsInvalidPathChar(char ch) =>
            _invalidPathChars.Contains(ch);

        private static string MakeFilePathPart(string name)
        {
            if (name.Any(IsInvalidPathChar))
            {
                var builder = new StringBuilder(name.Length);

                foreach (var ch in name)
                {
                    builder.Append(IsInvalidPathChar(ch) ? "_" : ch);
                }

                name = builder.ToString();
            }

            return name.ToLower();
        }

        private static DatabaseSymbol CreateDatabaseSymbol(DatabaseInfo db)
        {
            var members = new List<Symbol>();

            if (db.Tables != null)
                members.AddRange(db.Tables.Select(t => CreateTableSymbol(t)));
            if (db.ExternalTables != null)
                members.AddRange(db.ExternalTables.Select(e => CreateExternalTableSymbol(e)));
            if (db.MaterializedViews != null)
                members.AddRange(db.MaterializedViews.Select(v => CreateMaterializedViewSymbol(v)));
            if (db.Functions != null)
                members.AddRange(db.Functions.Select(f => CreateFunctionSymbol(f)));
            if (db.EntityGroups != null)
                members.AddRange(db.EntityGroups.Select(eg => CreateEntityGroupSymbol(eg)));
            if (db.GraphModels != null)
                members.AddRange(db.GraphModels.Select(gm => CreateGraphModelSymbol(gm)));

            return new DatabaseSymbol(db.Name, db.PrettyName, members);
        }

        private static TableSymbol CreateTableSymbol(TableInfo tab)
        {
            return new TableSymbol(tab.Name, tab.Schema, tab.Description);
        }

        private static ExternalTableSymbol CreateExternalTableSymbol(ExternalTableInfo xtab)
        {
            return new ExternalTableSymbol(xtab.Name, xtab.Schema, xtab.Description);
        }

        private static MaterializedViewSymbol CreateMaterializedViewSymbol(MaterializedViewInfo mview)
        {
            return new MaterializedViewSymbol(mview.Name, mview.Schema, mview.Query, mview.Description);
        }

        public static FunctionSymbol CreateFunctionSymbol(FunctionInfo fun)
        {
            return new FunctionSymbol(fun.Name, fun.Parameters, fun.Body, fun.Description);
        }

        public static EntityGroupSymbol CreateEntityGroupSymbol(EntityGroupInfo eg)
        {
            return new EntityGroupSymbol(eg.Name, eg.Definition);
        }

        public static GraphModelSymbol CreateGraphModelSymbol(GraphModelInfo gm)
        {
            return new GraphModelSymbol(gm.Name, gm.Edges, gm.Nodes, gm.Snapshots);
        }

        private static DatabaseInfo CreateDatabaseInfo(DatabaseSymbol db)
        {
            return new DatabaseInfo
            {
                Name = db.Name,
                PrettyName = db.AlternateName,
                Tables = db.Tables.Count > 0 ? db.Tables.Select(t => CreateTableInfo(t)).ToList() : null,
                ExternalTables = db.ExternalTables.Count > 0 ? db.ExternalTables.Select(e => CreateExternalTableInfo(e)).ToList() : null,
                MaterializedViews = db.MaterializedViews.Count > 0 ? db.MaterializedViews.Select(m => CreateMaterializedViewInfo(m)).ToList() : null,
                Functions = db.Functions.Count > 0 ? db.Functions.Select(f => CreateFunctionInfo(f)).ToList() : null,
                EntityGroups = db.EntityGroups.Count > 0 ? db.EntityGroups.Select(eg => CreateEntityGroupInfo(eg)).ToList() : null,
                GraphModels = db.GraphModels.Count > 0 ? db.GraphModels.Select(gm => CreateGraphModelInfo(gm)).ToList() : null
            };
        }

        private static TableInfo CreateTableInfo(TableSymbol symbol)
        {
            return new TableInfo
            {
                Name = symbol.Name,
                Schema = GetSchema(symbol),
                Description = string.IsNullOrEmpty(symbol.Description) ? null : symbol.Description
            };
        }

        private static ExternalTableInfo CreateExternalTableInfo(TableSymbol symbol)
        {
            return new ExternalTableInfo
            {
                Name = symbol.Name,
                Schema = GetSchema(symbol),
                Description = string.IsNullOrEmpty(symbol.Description) ? null : symbol.Description
            };
        }

        private static MaterializedViewInfo CreateMaterializedViewInfo(MaterializedViewSymbol symbol)
        {
            return new MaterializedViewInfo
            {
                Name = symbol.Name,
                Schema = GetSchema(symbol),
                Description = string.IsNullOrEmpty(symbol.Description) ? null : symbol.Description,
                Query = symbol.MaterializedViewQuery
            };
        }

        private static FunctionInfo CreateFunctionInfo(FunctionSymbol symbol)
        {
            return new FunctionInfo
            {
                Name = symbol.Name,
                Parameters = GetParameterList(symbol),
                Body = symbol.Signatures[0].Body,
                Description = string.IsNullOrEmpty(symbol.Description) ? null : symbol.Description
            };
        }

        private static EntityGroupInfo CreateEntityGroupInfo(EntityGroupSymbol symbol)
        {
            return new EntityGroupInfo
            {
                Name = symbol.Name,
                Definition = symbol.Definition
            };
        }

        private static GraphModelInfo CreateGraphModelInfo(GraphModelSymbol symbol)
        {
            return new GraphModelInfo
            {
                Name = symbol.Name,
                Edges = symbol.Edges.Select(s => s.Body).ToArray(),
                Nodes = symbol.Nodes.Select(s => s.Body).ToArray(),
                Snapshots = symbol.Snapshots.Select(s => s.Name).ToArray()
            };
        }

        public class DatabaseNameInfo
        {
            public string Name;
            public string PrettyName;
        }

        public class DatabaseInfo
        {
            public string Name;
            public string PrettyName;
            public List<TableInfo> Tables;
            public List<ExternalTableInfo> ExternalTables;
            public List<MaterializedViewInfo> MaterializedViews;
            public List<FunctionInfo> Functions;
            public List<EntityGroupInfo> EntityGroups;
            public List<GraphModelInfo> GraphModels;
        }

        public class TableInfo
        {
            public string Name;
            public string Schema;
            public string Description;
        }

        public class ExternalTableInfo
        {
            public string Name;
            public string Schema;
            public string Description;
        }

        public class MaterializedViewInfo
        {
            public string Name;
            public string Schema;
            public string Description;
            public string Query;
        }

        public class FunctionInfo
        {
            public string Name;
            public string Parameters;
            public string Body;
            public string Description;
        }

        public class EntityGroupInfo
        {
            public string Name;
            public string Definition;
        }

        public class GraphModelInfo
        {
            public string Name;
            public string[] Edges;
            public string[] Nodes;
            public string[] Snapshots;
        }
    }
}
