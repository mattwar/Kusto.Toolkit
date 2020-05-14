# Kushy
Tools for building Kusto query analyzers.

This project depends on:
1. Microsoft.Azure.Kusto.Language      -- Kusto parser and intellisense APIs
2. Microsoft.Azure.Kusto.Data          -- Kusto client query APIs

## SymbolLoader
Use the SymbolLoader class to feed the Kusto parser with database schemas directly from your cluster.


#### Discover databases available in a cluster
```csharp
var loader = new SymbolLoader(clusterConnectionString);
var names = loader.GetDatabaseNamesAsync();
```
</br>

#### Load database schema into a symbol
```csharp
var loader = new SymbolLoader(clusterConnectionString);
var db = await loader.LoadDatabaseAsync(dbName);
```
<br/>

#### Load database schema into a GlobalState and set it as the default database
```csharp
var globals = GlobalState.Default;
var loader = new SymbolLoader(clusterConnectionString);
var globalsWithDB = await loader.AddOrUpdateDefaultDatabaseAsync(globals, dbName);
var parsed = KustoCode.ParseAndAnalyze(query, globalsWithDB);
```
<br/>

#### Update KustoCode with schema for database() references found in the text
```csharp
// start with default database loaded
var loader = new SymbolLoader(clusterConnectionString);
var globals = await loader.AddOrUpdateDefaultDatabaseAsync(GlobalState.Default, dbName);

// parse query with references to other databases (not yet loaded)
var query = "database('somedb').Table | where x < y";
var code = KustoCode.ParseAndAnalyze(query, globals);

// load databases referenced by query not already part of globals
var updatedCode = await loader.AddReferencedDatabasesAsync(code);
var updatedGlobals = updatedCode.Globals;
```
<br/>

#### Update CodeScript with schema for database() references found in the text
```csharp
// start with an initial default database loaded
var loader = new SymbolLoader(clusterConnectionString);
var globals = await loader.AddOrUpdateDefaultDatabaseAsync(GlobalState.Default, dbName);

// parse query with references to other databases (not yet loaded)
var query = "database('somedb').Table | where x < y";
var script = CodeScript.From(query, globals);

// load databases referenced by query not already part of globals
var updatedScript = await loader.AddReferencedDatabasesAsync(script);
var updatedGlobals = updatedScript.Globals;
```

