# Kusto.Toolkit
Tools for building Kusto query analyzers.

This project is published at: 
https://www.nuget.org/packages/Kusto.Toolkit/

This project depends on:
1. Microsoft.Azure.Kusto.Language      -- Kusto parser and intellisense APIs
2. Microsoft.Azure.Kusto.Data          -- Kusto client query APIs
3. Newtonsoft.Json                     -- caching schema locally

## [SymbolLoader](src/Toolkit/docs/SymbolLoader.md)
Use the SymbolLoader family of classes to feed the Kusto parser with database schemas directly from your cluster.

1. ServerSymbolLoader -- loads symbols from a Kusto server using admin commands.
2. FileSymbolLoader -- loads and saves symbols into a file based symbol store.
3. CachedSymbolLoader -- loads symbols from file based cache or kusto server when not found locally.

## [SymbolResolver](src/Toolkit/docs/SymbolResolver.md)
Use the SymbolResolver class along with a SymbolLoader to load schema for cluster/database references in queries.
<br/>

## [KustoCode Extensions](src/Toolkit/docs/KustoCodeExtensions.md)
Use extension methods found in the `KustoCodeExtensions` class like `GetDatabaseTablesReferenced` and `GetDatabaseTableColumnsReferenced` to help you determine 
which tables and columns are used in a query, or use `GetSourceColumns` to determine which database table columns contributed to the content of any result columns.

## [GlobalState Extensions](src/Toolkit/docs/GlobalStateExtensions.md)
Use extension methods found in the `GlobalStateExtensions` class like `AddOrUpdateDatabaseMembers` to easily add or update tables or functions in the default database,
or used `ApplyCommand` to apply changes to schema symbols using commands.



