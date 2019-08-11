# Kushy
Tools for building Kusto query analyzers.

This project depends on:
1. Microsoft.Azure.Kusto.Language      -- Kusto parser and intellisense APIs
2. Microsoft.Azure.Kusto.Data          -- Kusto client query APIs

## SymbolLoader
Use the SymbolLoader class to feed the Kusto parser with database schemas directly from your cluster.


```csharp
// load database and add it manually to GlobalState
var loader = new SymbolLoader(clusterConnectionString);
var db = await loader.LoadDatabaseAsync(dbName);
var globalsWithDB = GlobalState.Default.WithDatabase(db);
var parsed = KustoCode.ParseAndAnalyze(query, globalsWithDB);
```

```csharp
// load database and add it automatically to GlobalState
var loader = new SymbolLoader(clusterConnectionString);
var globalsWithDB = await loader.AddOrUpdateDatabaseAsync(GlobalState.Default, dbName, asDefault: true);
var parsed = KustoCode.ParseAndAnalyze(query, globalsWithDB);
```

```csharp
// discover database names in cluster
var loader = new SymbolLoader(clusterConnectionString);
var names = loader.GetDatabaseNamesAsync();
```

```csharp
// load any databases referenced by query not already part of GlobalState
var loader = new SymbolLoader(clusterConnectionString);
var query = "database('somedb').Table | where x < y";
var code = KustoCode.ParseAndAnalyze(query, globals);
var updatedGlobals = await loader.AddReferencedDatabasesAsync(code);
var updatedCode = KustoCode.ParseAndAnalyze(query, updatedGlobals);
```
