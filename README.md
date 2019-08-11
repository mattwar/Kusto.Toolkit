# Kushy
Tools for building Kusto query analyzers.

This project depends on:
1. Microsoft.Azure.Kusto.Language      -- Kusto parser and intellisense APIs
2. Microsoft.Azure.Kusto.Data          -- Kusto client query APIs

## SymbolLoader
Use the SymbolLoader class to feed the Kusto parser with database schemas directly from your cluster.


```csharp
// load database, add it to globals and set it as default
var globals = GlobalState.Default;
var loader = new SymbolLoader(clusterConnectionString);
var globalsWithDB = await loader.AddOrUpdateDatabaseAsync(globals, dbName, asDefault: true);
var parsed = KustoCode.ParseAndAnalyze(query, globalsWithDB);
```

```csharp
// load database schema into a symbol
var loader = new SymbolLoader(clusterConnectionString);
var db = await loader.LoadDatabaseAsync(dbName);
```

```csharp
// discover database names in cluster
var loader = new SymbolLoader(clusterConnectionString);
var names = loader.GetDatabaseNamesAsync();
```

```csharp
// start with default database loaded
var loader = new SymbolLoader(clusterConnectionString);
var globals = await loader.AddOrUpdateDatabaseAsync(GlobalState.Default, dbName, asDefault: true);

// parse query with references to other databases
var query = "database('somedb').Table | where x < y";
var code = KustoCode.ParseAndAnalyze(query, globals);

// load any databases referenced by query not already part of globals
var updatedGlobals = await loader.AddReferencedDatabasesAsync(code);

// reparse using updated globals
var updatedCode = KustoCode.ParseAndAnalyze(query, updatedGlobals);
```
