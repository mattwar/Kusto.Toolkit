## KustoExtensions

These extension methods add additional functionality to your `KustoCode` instances.

### KustCode.GetDatabaseTablesReferenced()
Returns a list of all the database tables that are explicitly referenced in the query. 
This includes any table named explicitly or via the `table()` function, 
both in the body of the query and inside any function the query calls.

```csharp
var code = KustoCode.ParseAndAnalyze("TableA | join TableB on Id", globals);
var tables = code.GetDatabaseTablesReferenced();
// tables: TableA, TableB
```

### KustoCode.GetDatabaseTableColumnsReferenced()
Returns a list of all the database table columns explicitly referenced in the query.
This includes any column referencd in the body of the query and inside any function the query calls.

```csharp
var code = KustoCode.ParseAndAnalyze("TableA | where X > Y | project A, B, C", globals);
var columns = code.GetDatabaseTablesReferenced();
// columns: X, Y, A, B, C
```

### KustoCode.GetDatabaseTableColumnsInResult()
Returns a list of only the database table columns in the query result, 
which may be different than all the columns in the query result since the 
result may contain computed columns.

```csharp
var code = KustoCode.ParseAndAnalyze("TableA | where X > Y | project A, B, V=C*D", globals);
var columns = code.GetDatabaseTablesColumnsInResult();
// columns: A, B
```

### KustoCode.GetResultColumns()
Returns all the columns in the result of the query. 

```csharp
// TableA: A, B, C, D, X, Y, Z
var code = KustoCode.ParseAndAnalyze("TableA | where X > Y | extend V=C*D", globals);
var columns = code.GetResultColumns();
// columns: A, B, C, D, X, Y, Z, V
```

### KustoCode.GetSourceColumns()
Returns the set of database table columns that contributed to the data contained in the result columns.

```csharp
var code = KustoCode.ParseAndAnalyze("TableA | where X > Y | project AB=A+B, CD=C+D", globals);
var columns = code.GetSourceColumns();
// columns: A, B, C, D
```

### KustoCode.GetSourceColumns(ColumnSymbol column)
Returns the set of database table columns that contributed to the data contained in the specified column.

```csharp
var code = KustoCode.ParseAndAnalyze("TableA | where X > Y | project AB=A+B, CD=C+D", globals);
var resultColumn = code.GetResultColumns().First(c => c.Name == "AB");
var columns = code.GetSourceColumns(resultColumn);
// columns: A, B
```

### KustoCode.GetSourceColumns(IReadOnlyList&lt;ColumnSymbol&gt; columns)
Returns the set of database table columns that contributed to the data contained in the specified columns.

```csharp
var code = KustoCode.ParseAndAnalyze("TableA | where X > Y | project AB=A+B, AC=A+C, BC=B+C", globals);
var resultColumns = code.GetResultColumns().First(c => c.Name.StartsWith("A")).ToList();
var columns = code.GetSourceColumns(resultColumns);
// columns: A, B, C
```

### KustoCode.GetSourceColumnMap()
Returns the a map between the query's result columns and the set of database table columns that contributed to them.

```csharp
var code = KustoCode.ParseAndAnalyze("TableA | where X > Y | project AB=A+B, AC=A+C, BC=B+C", globals);
var map = code.GetSourceColumnMap();
// map: AB: [A, B], AC: [A, C], BC: [B, C]
```

### KustoCode.GetSourceColumnMap(IReadOnlyList&lt;ColumnSymbol&gt; columns)
Returns the a map between the specified columns and the set of database table columns that contributed to them.

```csharp
var code = KustoCode.ParseAndAnalyze("TableA | where X > Y | project AB=A+B, AC=A+C, BC=B+C", globals);
var resultColumns = code.GetResultColumns().First(c => c.Name.StartsWith("A")).ToList();
var columns = code.GetSourceColumnMap(resultColumns);
// map: AB: [A, B], AC: [A, C]
```

<br/>

## Not extensions, but still useful

### GetDatabaseTableColumnsReferenced(SyntaxNode root, GlobalState globals)
This is a non-extension version of `GeDatabaseTableColumns()` that you can use to 
discover the columns referenced in a sub-tree of the entire query.  

```csharp
var code = KustoCode.ParseAndAnalyze("TableA | where X > Y | project A, B, C", globals);
// only get database table columns referenced in the project operator 
var projectNode = code.GetFirstDescendant<ProjectOperator>();
var columns = KustoExtensions.GetDatabaseTableColumnsReferenced(projectNode, code.Globals);
// columns: A, B, C
```

### GetColumnsReferenced(SyntaxNode node)
Returns a list of all columns referenced or declared in the sub-tree, or 
inside any function called within the sub-tree.

```csharp
var code = KustoCode.ParseAndAnalyze("TableA | extend YY=Y*Y | where X > YY | project A, B, C", globals);
// only get columns referenced in the where/filter operator
var whereNode = code.GetFirstDescendant<FilterOperator>();
var columns = KustoExtensions.GetColumnsReferenced(projectNode);
// columns: X, YY
```

