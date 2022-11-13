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
            _schemaDirectoryPath = Environment.ExpandEnvironmentVariables(schemaDirectoryPath);
            _defaultClusterName = defaultClusterName;
            _defaultDomain = defaultDomain ?? KustoFacts.KustoWindowsNet;
        }

        public override string DefaultCluster => _defaultClusterName;
        public override string DefaultDomain => _defaultDomain;

        public override Task<IReadOnlyList<DatabaseName>> GetDatabaseNamesAsync(string clusterName = null, bool throwOnError = false, CancellationToken cancellationToken = default)
        {
            return LoadDatabaseNamesAsync(clusterName, throwOnError, cancellationToken);
        }

        /// <summary>
        /// Loads the named database schema from the cache.
        /// </summary>
        public override async Task<DatabaseSymbol> LoadDatabaseAsync(string databaseName, string clusterName = null, bool throwOnError = false, CancellationToken cancellationToken = default)
        {
            if (databaseName == null)
                throw new ArgumentNullException(nameof(databaseName));

            if (string.IsNullOrWhiteSpace(databaseName))
                throw new ArgumentException("Invalid database name", nameof(databaseName));

            var databasePath = GetDatabaseCachePath(databaseName, clusterName);

            if (databasePath != null)
            {
                try
                {
                    if (File.Exists(databasePath))
                    {
                        var jsonText = await File.ReadAllTextAsync(databasePath).ConfigureAwait(false);
                        var dbInfo = JsonConvert.DeserializeObject<DatabaseInfo>(jsonText);
                        return CreateDatabaseSymbol(dbInfo);
                    }
                }
                catch (Exception) when (!throwOnError)
                {
                }
            }

            return null;
        }

        /// <summary>
        /// Saves the database schema to the cache.
        /// </summary>
        public async Task<bool> SaveDatabaseAsync(DatabaseSymbol database, string clusterName = null, bool throwOnError = false, CancellationToken cancellationToken = default)
        {
            if (database == null)
                throw new ArgumentNullException(nameof(database));

            var clusterPath = GetClusterCachePath(clusterName);
            var databasePath = GetDatabaseCachePath(database.Name, clusterName);

            if (clusterPath != null && databasePath != null)
            {
                try
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

                    await File.WriteAllTextAsync(databasePath, jsonText, cancellationToken).ConfigureAwait(false);
                    var dbName = new DatabaseName(database.Name, database.AlternateName);
                    await SaveDatabaseNameAsync(clusterName, dbName, throwOnError, cancellationToken).ConfigureAwait(false);

                    return true;
                }
                catch (Exception) when (!throwOnError)
                {
                }
            }

            return false;
        }

        /// <summary>
        /// Saves all the cluster schema to the cache.
        /// </summary>
        public async Task SaveClusterAsync(ClusterSymbol cluster, CancellationToken cancellationToken = default)
        {
            if (cluster == null)
                throw new ArgumentNullException(nameof(cluster));

            foreach (var db in cluster.Databases)
            {
                var _ = await SaveDatabaseAsync(db, cluster.Name, throwOnError: false, cancellationToken: cancellationToken).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Saves all the schema for all the clusters to the cache.
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

        /// <summary>
        /// Returns the list of saved database names for the cluster.
        /// </summary>
        public async Task<IReadOnlyList<DatabaseName>> LoadDatabaseNamesAsync(string clusterName, bool throwOnError = false, CancellationToken cancellationToken = default)
        {
            if (clusterName == null)
                throw new ArgumentNullException(nameof(clusterName));

            var dbNamesPath = GetDatabaseNamesFilePath(clusterName);
            if (dbNamesPath != null && File.Exists(dbNamesPath))
            {
                try
                {
                    var jsonText = await File.ReadAllTextAsync(dbNamesPath).ConfigureAwait(false);
                    var dbNames = JsonConvert.DeserializeObject<DatabaseNameInfo[]>(jsonText);
                    if (dbNames != null)
                    {
                        return dbNames.Select(info => new DatabaseName(info.Name, info.PrettyName)).ToArray();
                    }
                }
                catch (Exception) when (!throwOnError)
                {
                }
            }

            return null;
        }

        /// <summary>
        /// Saves and overwrites the entire list of database names.
        /// Returns true if successful or false if not.
        /// </summary>
        public async Task<bool> SaveDatabaseNamesAsync(string clusterName, IReadOnlyList<DatabaseName> databaseNames, bool throwOnError = false, CancellationToken cancellationToken = default)
        {
            if (clusterName == null)
                throw new ArgumentNullException(nameof(clusterName));

            if (databaseNames == null)
                throw new ArgumentNullException(nameof(databaseNames));

            var dbNamesPath = GetDatabaseNamesFilePath(clusterName);
            var parentDir = Path.GetDirectoryName(dbNamesPath);

            try
            {
                if (!Directory.Exists(parentDir))
                {
                    Directory.CreateDirectory(parentDir);
                }

                var dbNameInfos = databaseNames.Select(dn => new DatabaseNameInfo { Name = dn.Name, PrettyName = dn.PrettyName }).ToArray();
                var jsonText = JsonConvert.SerializeObject(dbNameInfos, s_serializationSettings);
                await File.WriteAllTextAsync(dbNamesPath, jsonText, cancellationToken).ConfigureAwait(false);

                return true;
            }
            catch (Exception) when (!throwOnError)
            {
            }

            return false;
        }

        /// <summary>
        /// Adds the database name to the list of saved database names.
        /// Returns true if successful or false if not.
        /// </summary>
        public async Task<bool> SaveDatabaseNameAsync(string clusterName, DatabaseName databaseName, bool throwOnError = false, CancellationToken cancellationToken = default)
        {
            if (clusterName == null)
                throw new ArgumentNullException(nameof(clusterName));

            if (databaseName == null)
                throw new ArgumentNullException(nameof(databaseName));

            if (String.IsNullOrEmpty(databaseName.Name))
            {
                return false;
            }

            try
            {
                var dbNames = await LoadDatabaseNamesAsync(clusterName, true, cancellationToken).ConfigureAwait(false);
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
                    await SaveDatabaseNamesAsync(clusterName, dbNames, throwOnError, cancellationToken).ConfigureAwait(false);
                }

                return true;
            }
            catch (Exception) when (!throwOnError)
            {
            }

            return false;
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
        /// </summary>
        public string GetDatabaseNamesFilePath(string clusterName)
        {
            var clusterPath = GetClusterCachePath(clusterName);
            
            if (clusterPath != null)
            {
                return Path.Combine(clusterPath, "databaseNames.json");
            }

            return null;
        }

        /// <summary>
        /// Gets path to the cluster cache directory within the schema cache directory.
        /// </summary>
        public string GetClusterCachePath(string clusterName = null)
        {
            clusterName = clusterName ?? _defaultClusterName;
            if (clusterName == null)
                return null;

            var fullClusterName = GetFullHostName(clusterName, _defaultDomain);
            return Path.Combine(_schemaDirectoryPath, MakeFilePathPart(fullClusterName));
        }

        /// <summary>
        /// Gets the path to the database schema file.
        /// </summary>
        public string GetDatabaseCachePath(string databaseName, string clusterName = null)
        {
            if (databaseName == null)
                throw new ArgumentNullException(nameof(databaseName));

            clusterName = clusterName ?? _defaultClusterName;
            if (clusterName == null)
                return null;

            return Path.Combine(GetClusterCachePath(clusterName), MakeFilePathPart(databaseName) + ".json");
        }

        private static bool IsInvalidPathChar(char ch) =>
            ch == '\\' || ch == '/';

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
                EntityGroups = db.EntityGroups.Count > 0 ? db.EntityGroups.Select(eg => CreateEntityGroupInfo(eg)).ToList() : null
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
    }
}
