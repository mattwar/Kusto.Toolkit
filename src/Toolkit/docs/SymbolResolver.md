## SymbolResolver
Use the `SymbolResolver` class along with a `SymbolLoader` to load additional schema symbols 
for databases that are referenced using the `database()` function.

#### Update KustoCode database references
```csharp
// start with default database loaded
var connection = new KustoConnectionStringBuilder(...);
var loader = new ServerSymbolLoader(connection);
var globals = await loader.AddOrUpdateDefaultDatabaseAsync(GlobalState.Default, "primary_db");

// parse query with references to other databases (not yet loaded)
var query = "database('other_db').Table | where x < y";
var code = KustoCode.ParseAndAnalyze(query, globals);

// load databases referenced by query that are not already included in globals
var resolver = new SymbolResolver(loader);
var updatedCode = await resolver.AddReferencedDatabasesAsync(code);
```

#### Update CodeScript database references
```csharp
// start with an initial default database loaded
var connection = new KustoConnectionStringBuilder(...);
var loader = new ServerSymbolLoader(connection);
var globals = await loader.AddOrUpdateDefaultDatabaseAsync(GlobalState.Default, "primary_db");

// parse query with references to other databases (not yet loaded)
var query = "database('other_db').Table | where x < y";
var script = CodeScript.From(query, globals);

// load databases referenced by query that are not already included in globals
var resolver = new SymbolResolver(loader);
var updatedScript = await resolver.AddReferencedDatabasesAsync(script);
```


