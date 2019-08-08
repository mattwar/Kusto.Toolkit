# Kushy
Tools for building Kusto query analyzers.

This project depends on:
1. Microsoft.Azure.Kusto.Language      -- Kusto parser and intellisense APIs
2. Microsoft.Azure.Kusto.Data          -- Kusto client query APIs

## SymbolLoader
Use the SymbolLoader class to feed the Kusto parser with database schemas directly from your cluster.

```csharp
var loader = new SymbolLoader(clusterConnectionString);
var db = await loader.GetDatabaseSymbolAsync(dbName);
var globalsWithDB = GlobalState.Default.WithDatabase(db);
var parsed = KustoCode.ParseAndAnalyze(query, globalsWithDB);
```
