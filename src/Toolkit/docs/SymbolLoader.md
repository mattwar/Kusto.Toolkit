# SymbolLoader
Use the SymbolLoader family of classes to feed the Kusto parser with database schemas directly from your cluster.

### ServerSymbolLoader
The `ServerSymbolLoader` class loads symbols from a Kusto server using admin commands to request schema.
</br>

#### Discover databases available in a cluster
```csharp
var loader = new ServerSymbolLoader(clusterConnectionString);
var names = loader.LoadDatabaseNamesAsync();
```
</br>

#### Load database schema into a symbol
```csharp
var loader = new ServerSymbolLoader(clusterConnectionString);
var db = await loader.LoadDatabaseAsync(dbName);
```
<br/>

#### Load database schema into a GlobalState and set it as the default database
```csharp
var globals = GlobalState.Default;
var loader = new ServerSymbolLoader(clusterConnectionString);
var globalsWithDB = await loader.AddOrUpdateDefaultDatabaseAsync(globals, dbName);
var parsed = KustoCode.ParseAndAnalyze(query, globalsWithDB);
```
<br/>

### FileSymbolLoader
The `FileSymbolLoader` class loads and saves symbols into a file based symbol store.
<br/>

#### Load symbols directly from a file symbol loader (no server involved)
```csharp
var loader = new FileSymbolLoader(schemaCacheDirectoryPath, defaultClusterName);
var db = await loader.LoadDatabaseAsync(dbName);
```
<br/>

#### Delete all cached schema
```csharp
var loader = new FileSymbolLoader(schemaCacheDirectoryPath, defaultClusterName);
loader.DeleteCache();
```
<br/>

#### Delete all cached schema from a cached server symbol loader.
```csharp
var loader = new CachedServerSymbolLoader(clusterConnectionString, schemaCacheDirectoryPath);
loader.FileLoader.DeleteCache();
```
<br/>

#### Delete cached schema for a single cluster only
```csharp
var loader = new FileSymbolLoader(schemaCacheDirectoryPath, defaultClusterName);
loader.DeleteClusterCache(clusterName);
```
<br/>

#### Delete cached schema for the default cluster only
```csharp
var loader = new FileSymbolLoader(schemaCacheDirectoryPath, defaultClusterName);
loader.DeleteClusterCache();
```
<br/>

#### Manually store schema for database symbol into a file based schema store.
```csharp
var loader = new FileSymbolLoader(schemaCacheDirectoryPath, defaultClusterName);
var databaseSymbol = ...;
await loader.SaveDatabaseAsync(databaseSymbol, clusterName);
```
<br/>

#### Manually store schema for entire cluster symbol into a file based schema store.
```csharp
var loader = new FileSymbolLoader(schemaCacheDirectoryPath, defaultClusterName);
var clusterSymbol = ...;
await loader.SaveClusterAsync(clusterSymbol);
```
<br/>

#### Manually store schema for multiple cluster symbols into a file based schema store.
```csharp
var loader = new FileSymbolLoader(schemaCacheDirectoryPath, defaultClusterName);
var clusterSymbols = new ClusterSymbol[] { ... };
await loader.SaveClustersAsync(clusterSymbols);
```
<br/>

### CachedSymbolLoader

The `CachedSymbolLoader` class combines both the `ServerSymbolLoader` and `FileSymbolLoader` classes togther
to make a `SymbolLoader` that loads symbols from either a file based cache or a kusto server when not found in the cache.
<br/>

#### Use a cached symbol loader to speed up repeated symbol loading
```csharp
var loader = new CachedSymbolLoader(clusterConnectionString, schemaCacheDirectoryPath);
var db = await loader.LoadDatabaseAsync(dbName);
```
<br/>
