# Kusto.Toolkit

This library contains tools to help you load database schemas as 
the symbols you need to succesfully parse and analyze Kusto queries.

This project depends on:
1. Microsoft.Azure.Kusto.Language      -- Kusto parser and intellisense APIs
2. Microsoft.Azure.Kusto.Data          -- Kusto client query APIs
3. Newtonsoft.Json                     -- caching schema locally

## SymbolLoader
Use the SymbolLoader family of classes to feed the Kusto parser with database schemas directly from your cluster.

## SymbolResolver
Use the SymbolResolver class along with a SymbolLoader to load schema for cluster/database references in queries.

## KustoExtensions
Use extension methods found in the `KustoExtensions` class like `GetDatabaseTablesReferenced` and `GetDatabaseTableColumnsReferenced` to help you determine 
which tables and columns are used in a query, or use `GetSourceColumns` to determine which database table columns contributed to the content of any result columns.


## Access the source code, contribute or just ask questions:
https://github.com/mattwar/Kushy



