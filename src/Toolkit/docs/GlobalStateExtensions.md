## GlobalState Extensions

These extension methods add additional functionality to your `GlobalState` instances.

### GlobalState.AddOrUpdateDatabaseMembers(params Symbol[] newMembers)

Add or update members of the default database in one step.

### GlobalState.RemoveDatabaseMembers(params Symbol[] members)

Remove members of the default database.

### GlobalState.ReplaceDatabaseMembers&lt;TMember&gt;(params (TMember original, TMember replacement)[] replacements)

Replace members of the default database with updated members.
This is similar to updating members with `AddOrUpdateDatabaseMembers`, but also works when the member names have changed.

### GlobalState.AddOrUpdateClusterDatabase(DatabaseSymbol newDatabase)

Add or update a database within the default cluster.

### GlobalState.AddOrUpdateClusterDatabases(params DatabaseSymbol[] newDatabases)

Add or update multiple databases within the default cluster.

<br/>

### GlobalState.ApplyCommand(string command)
You may want to evaluate a query or command in the context of another command having already been executed,
such a table or function being created or altered, but without actually executing the command.

Instead of having to manually construct the appropiate symbols corresponding to each change 
you can now just apply the command text directly to the global state and get a new instance
with the related schema modified for you.

```csharp
var globals = GlobalState.Default.ApplyCommand(".create table Customers (Id: long, Name: string)");
var code = KustoCode.ParseAndAnalyze("Customers | where Name == 'Matt'");
```

### GlobalState.ApplyCommands(params string[] commands)

To apply more than one command a time, in order, use `GlobalState.ApplyCommands`.

```csharp
var globals = _globals.ApplyCommands(
    ".create table T (x: long, y: string)",
    ".set T2 <| T extend z=1.0");
```

*note: it is also possible to apply more than one command at a time by applying a single execute script command.*