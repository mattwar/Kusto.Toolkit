# Kushy
Tools for building Kusto query analyzers.

This project depends on:
1. Microsoft.Azure.Kusto.Language      -- Kusto parser and intellisense APIs
2. Microsoft.Azure.Kusto.Data          -- Kusto client query APIs
3. Newtonsoft.Json                     -- caching schema locally

## SymbolLoader
Use the SymbolLoader family of classes to feed the Kusto parser with database schemas directly from your cluster.

1. ServerSymbolLoader -- loads symbols from a Kusto server using admin commands.
2. FileSymbolLoader -- loads and saves symbols into a file based symbol store.
3. CachedServerSymbolLoader -- loads symbols from file based cache or kusto server when not found locally.

## SymbolResolver
Use the SymbolResolver class along with a SymbolLoader to load schema for cluster/database references in queries.

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

#### Update KustoCode with schema for database() references found in the text
```csharp
// start with default database loaded
var loader = new ServerSymbolLoader(clusterConnectionString);
var globals = await loader.AddOrUpdateDefaultDatabaseAsync(GlobalState.Default, dbName);

// parse query with references to other databases (not yet loaded)
var query = "database('somedb').Table | where x < y";
var code = KustoCode.ParseAndAnalyze(query, globals);

// load databases referenced by query that are not already included in globals
var resolver = new SymbolResolver(loader);
var updatedCode = await resolver.AddReferencedDatabasesAsync(code);
var updatedGlobals = updatedCode.Globals;
```
<br/>

#### Update CodeScript with schema for database() references found in the text
```csharp
// start with an initial default database loaded
var loader = new ServerSymbolLoader(clusterConnectionString);
var globals = await loader.AddOrUpdateDefaultDatabaseAsync(GlobalState.Default, dbName);

// parse query with references to other databases (not yet loaded)
var query = "database('somedb').Table | where x < y";
var script = CodeScript.From(query, globals);

// load databases referenced by query that are not already included in globals
var resolver = new SymbolResolver(loader);
var updatedScript = await resolver.AddReferencedDatabasesAsync(script);
var updatedGlobals = updatedScript.Globals;
```
<br/>

#### Use a cached symbol loader to speed up repeated symbol loading
```csharp
var loader = new CachedServerSymbolLoader(clusterConnectionString, schemaCacheDirectoryPath);
var db = await loader.LoadDatabaseAsync(dbName);
```
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