# Kusto.Toolkit

This library contains tools to help you load database schemas as 
the symbols you need to succesfully parse and analyze Kusto queries.

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


## Access the source code, contribute or just ask questions:
https://github.com/mattwar/Kushy



