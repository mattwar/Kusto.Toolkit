using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using Kusto.Language;
using Kusto.Language.Symbols;
using Kusto.Language.Editor;
using Kusto.Language.Syntax;

namespace Tests
{
    public static partial class GlobalStateExtensions
    {
        /// <summary>
        /// Returns a new <see cref="GlobalState"/> modified by the commands in order.
        /// The default cluster and database must already be set or no nothing will be applied.
        /// </summary>
        public static GlobalState ApplyCommands(this GlobalState globals, IEnumerable<string> commands)
        {
            foreach (var command in commands)
            {
                globals = globals.ApplyCommand(command);
            }

            return globals;
        }

        /// <summary>
        /// Returns a new <see cref="GlobalState"/> modified by the commands in order.
        /// The default cluster and database must already be set or no nothing will be applied.
        /// </summary>
        public static GlobalState ApplyCommands(this GlobalState globals, params string[] commands)
        {
            return globals.ApplyCommands((IEnumerable<string>)commands);
        }

        /// <summary>
        /// Returns a new <see cref="GlobalState"/> modified by the command.
        /// The default cluster and database must already be set or no nothing will be applied.
        /// </summary>
        public static GlobalState ApplyCommand(this GlobalState globals, string command)
        {
            // cannot add members to unknown cluster or database
            if (globals.Cluster == ClusterSymbol.Unknown
                || globals.Database == DatabaseSymbol.Unknown)
                return globals;

            // if not a command, then it is not a schema altering command
            if (KustoCode.GetKind(command) != CodeKinds.Command)
                return globals;

            var code = KustoCode.Parse(command, globals);
            var commandRoot = code.Syntax.GetFirstDescendant<CustomCommand>();
            if (commandRoot == null)
                return globals;

            return commandRoot.CommandKind switch
            {
                // functions
                nameof(EngineCommands.CreateFunction) =>
                    ApplyCreateFunction(code, commandRoot),
                nameof(EngineCommands.AlterFunction) =>
                    ApplyAlterFunction(code, commandRoot),
                nameof(EngineCommands.CreateOrAlterFunction) =>
                    ApplyCreateOrAlterFunction(code, commandRoot),
                nameof(EngineCommands.AlterFunctionDocString) =>
                    ApplyAlterFunctionDocString(code, commandRoot),

                // tables
                nameof(EngineCommands.CreateTable) or
                nameof(EngineCommands.CreateTables) or
                nameof(EngineCommands.DefineTable) or
                nameof(EngineCommands.DefineTables) =>
                    ApplyCreateTable(code, commandRoot),
                nameof(EngineCommands.CreateMergeTable) or
                nameof(EngineCommands.CreateMergeTables) =>
                    ApplyCreateMergeTable(code, commandRoot),
                nameof(EngineCommands.CreateTableBasedOnAnother) =>
                    ApplyCreateTableBaseOnAnotherTable(code, commandRoot),
                nameof(EngineCommands.AlterTable) =>
                    ApplyAlterTable(code, commandRoot),
                nameof(EngineCommands.AlterMergeTable) =>
                    ApplyAlterMergeTable(code, commandRoot),
                nameof(EngineCommands.AlterTableDocString) =>
                    ApplyAlterTableDocString(code, commandRoot),
                nameof(EngineCommands.RenameTable) or
                nameof(EngineCommands.RenameTables) =>
                    ApplyRenameTable(code, commandRoot),
                nameof(EngineCommands.DropTable) or
                nameof(EngineCommands.DropTables) =>
                    ApplyDropTable(code, commandRoot),
                nameof(EngineCommands.SetTable) =>
                    ApplySetTable(code, commandRoot),
                nameof(EngineCommands.SetOrAppendTable) =>
                    ApplySetOrAppendTable(code, commandRoot),
                nameof(EngineCommands.SetOrReplaceTable) =>
                    ApplySetOrReplaceTable(code, commandRoot),
                nameof(EngineCommands.AppendTable) =>
                    ApplyAppendTable(code, commandRoot),

                // columns
                nameof(EngineCommands.AlterColumnType) =>
                    ApplyAlterColumnType(code, commandRoot),
                nameof(EngineCommands.DropColumn) =>
                    ApplyDropColumn(code, commandRoot),
                nameof(EngineCommands.DropTableColumns) =>
                    ApplyDropTableColumns(code, commandRoot),
                nameof(EngineCommands.RenameColumn) =>
                    ApplyRenameColumn(code, commandRoot),
                nameof(EngineCommands.RenameColumns) =>
                    ApplyRenameColumns(code, commandRoot),
                nameof(EngineCommands.AlterTableColumnDocStrings) =>
                    ApplyAlterTableColumnDocStrings(code, commandRoot),
                nameof(EngineCommands.AlterMergeTableColumnDocStrings) =>
                    ApplyAlterMergeTableColumnDocStrings(code, commandRoot),

                // external tables
                nameof(EngineCommands.CreateStorageExternalTable) or
                nameof(EngineCommands.CreateSqlExternalTable) =>
                    ApplyCreateExternalTable(code, commandRoot),
                nameof(EngineCommands.AlterStorageExternalTable) or
                nameof(EngineCommands.AlterSqlExternalTable) =>
                    ApplyAlterExternalTable(code, commandRoot),
                nameof(EngineCommands.CreateOrAlterStorageExternalTable) or
                nameof(EngineCommands.CreateOrAlterSqlExternalTable) =>
                    ApplyCreateOrAlterExternalTable(code, commandRoot),
                nameof(EngineCommands.DropExternalTable) =>
                    ApplyDropExternalTable(code, commandRoot),

                // materialized views
                nameof(EngineCommands.CreateMaterializedView) or
                nameof(EngineCommands.CreateMaterializedViewOverMaterializedView) =>
                    ApplyCreateMaterializedView(code, commandRoot),
                nameof(EngineCommands.AlterMaterializedView) or
                nameof(EngineCommands.AlterMaterializedViewOverMaterializedView) =>
                    ApplyAlterMaterializedView(code, commandRoot),
                nameof(EngineCommands.CreateOrAlterMaterializedView) =>
                    ApplyCreateOrAlterMaterializedView(code, commandRoot),
                nameof(EngineCommands.AlterMaterializedViewDocString) =>
                    ApplyAlterMaterializedViewDocString(code, commandRoot),
                nameof(EngineCommands.DropMaterializedView) =>
                    ApplyDropMaterializedView(code, commandRoot),
                nameof(EngineCommands.RenameMaterializedView) =>
                    ApplyRenameMaterializedView(code, commandRoot),

                // scripts
                nameof(EngineCommands.ExecuteDatabaseScript) =>
                    ApplyExecuteDatabaseScript(code, commandRoot),
                _ => globals
            };
        }

        private static GlobalState ApplyCreateFunction(KustoCode code, CustomCommand commandRoot)
        {
            var name = commandRoot.GetFirstDescendant<NameDeclaration>(nd => nd.NameInParent == "FunctionName")?.SimpleName;
            var parameters = commandRoot.GetFirstDescendant<FunctionParameters>()?.ToString();
            var body = commandRoot.GetFirstDescendant<FunctionBody>()?.ToString();
            var docstring = GetPropertyValueText(commandRoot, "docstring");

            if (name != null && parameters != null && body != null
                && code.Globals.Database.GetFunction(name) == null)
            {
                var newFunction = new FunctionSymbol(name, parameters, body, docstring);
                return code.Globals.AddOrUpdateDatabaseMembers(newFunction);
            }

            return code.Globals;
        }

        private static GlobalState ApplyAlterFunction(KustoCode code, CustomCommand commandRoot)
        {
            var name = commandRoot.GetFirstDescendant<NameReference>(nr => nr.GetLastToken().GetNextToken() is SyntaxToken tk && tk.Kind == SyntaxKind.OpenParenToken)?.SimpleName;
            var parameters = commandRoot.GetFirstDescendant<FunctionParameters>()?.ToString();
            var body = commandRoot.GetFirstDescendant<FunctionBody>()?.ToString();
            var docstring = GetPropertyValueText(commandRoot, "docstring");

            if (name != null && parameters != null && body != null
                && code.Globals.Database.GetFunction(name) != null)
            {
                var newFunction = new FunctionSymbol(name, parameters, body, docstring);
                return code.Globals.AddOrUpdateDatabaseMembers(newFunction);
            }

            return code.Globals;
        }

        private static GlobalState ApplyCreateOrAlterFunction(KustoCode code, CustomCommand commandRoot)
        {
            var name = commandRoot.GetFirstDescendant<NameDeclaration>(nd => nd.GetLastToken().GetNextToken() is SyntaxToken tk && tk.Kind == SyntaxKind.OpenParenToken)?.SimpleName;
            var parameters = commandRoot.GetFirstDescendant<FunctionParameters>()?.ToString();
            var body = commandRoot.GetFirstDescendant<FunctionBody>()?.ToString();
            var docstring = GetPropertyValueText(commandRoot, "docstring");

            if (name != null && parameters != null && body != null)
            {
                var newFunction = new FunctionSymbol(name, parameters, body, docstring);
                return code.Globals.AddOrUpdateDatabaseMembers(newFunction);
            }

            return code.Globals;
        }

        private static GlobalState ApplyAlterFunctionDocString(KustoCode code, CustomCommand commandRoot)
        {
            var name = GetNameAfterToken(commandRoot, "function");
            var description = GetLiteralValueAfterToken(commandRoot, "docstring");
            if (name != null && description != null
                && code.Globals.Database.GetFunction(name) is FunctionSymbol fn)
            {
                var newFn = fn.WithDescription(description);
                return code.Globals.AddOrUpdateDatabaseMembers(newFn);
            }

            return code.Globals;
        }

        private static GlobalState ApplyCreateTable(KustoCode code, CustomCommand commandRoot)
        {
            var nameNodes = commandRoot.GetDescendants<NameDeclaration>(nd => nd.NameInParent == "TableName");
            var docstring = GetPropertyValueText(commandRoot, "docstring");

            var newTables = nameNodes.Select(nn =>
            {
                var name = nn.SimpleName;
                var schema = GetSchemaAfterElement(code, nn);
                if (schema != null)
                {
                    return new TableSymbol(name, schema, docstring);
                }
                return null;
            })
            .Where(nt => nt != null)
            .ToList();

            return code.Globals.AddOrUpdateDatabaseMembers(newTables!);
        }

        private static GlobalState ApplyCreateMergeTable(KustoCode code, CustomCommand commandRoot)
        {
            var nameNodes = commandRoot.GetDescendants<NameDeclaration>(nd => nd.NameInParent == "TableName");
            var docstring = GetPropertyValueText(commandRoot, "docstring");

            var newTables = nameNodes.Select(nn =>
            {
                var name = nn.SimpleName;

                var schema = GetSchemaAfterElement(code, nn);
                if (schema != null)
                {
                    var newTable = new TableSymbol(name, schema, docstring);

                    if (code.Globals.Database.GetTable(name) is TableSymbol existingTable)
                    {
                        var mergedColumns = MergeColumns(existingTable.Columns, newTable.Columns);
                        if (mergedColumns != existingTable.Columns)
                        {
                            docstring = docstring ?? existingTable.Description;
                            return new TableSymbol(name, mergedColumns, docstring);
                        }
                    }
                    else
                    {
                        return newTable;
                    }
                }

                return null;
            })
            .Where(nt => nt != null)
            .ToList();

            return code.Globals.AddOrUpdateDatabaseMembers(newTables!);
        }

        private static GlobalState ApplyCreateTableBaseOnAnotherTable(KustoCode code, CustomCommand commandRoot)
        {
            var existingTableName = commandRoot.GetFirstDescendant<NameDeclaration>(nd => nd.NameInParent == "TableName")?.SimpleName;
            var newTableName = commandRoot.GetFirstDescendant<NameDeclaration>(nd => nd.NameInParent == "NewTableName")?.SimpleName;

            if (existingTableName != null
                && newTableName != null
                && code.Globals.Database.GetTable(existingTableName) is TableSymbol existingTableSymbol)
            {
                var newTableSymbol = existingTableSymbol.WithName(newTableName);
                return code.Globals.AddOrUpdateDatabaseMembers(newTableSymbol);
            }

            return code.Globals;
        }

        private static GlobalState ApplyAlterTable(KustoCode code, CustomCommand commandRoot)
        {
            var tableNameNode = commandRoot.GetFirstDescendant<NameReference>();
            var tableName = tableNameNode?.SimpleName;
            var tableSchema = GetSchemaAfterElement(code, tableNameNode);
            var docstring = GetPropertyValueText(commandRoot, "docstring");

            if (tableName != null
                && tableSchema != null
                && code.Globals.Database.GetTable(tableName) is TableSymbol existingTable)
            {
                var newTable = new TableSymbol(tableName, tableSchema, docstring ?? existingTable.Description);
                return code.Globals.AddOrUpdateDatabaseMembers(newTable);
            }

            return code.Globals;
        }

        private static GlobalState ApplyAlterMergeTable(KustoCode code, CustomCommand commandRoot)
        {
            var tableNameNode = commandRoot.GetFirstDescendant<NameReference>();
            var tableName = tableNameNode?.SimpleName;
            var tableSchema = GetSchemaAfterElement(code, tableNameNode);
            var docstring = GetPropertyValueText(commandRoot, "docstring");

            if (tableName != null
                && tableSchema != null)
            {
                var newTable = new TableSymbol(tableName, tableSchema, docstring);

                if (code.Globals.Database.GetTable(tableName) is TableSymbol existingTable)
                {
                    var mergedColumns = MergeColumns(existingTable.Columns, newTable.Columns);
                    if (mergedColumns != existingTable.Columns)
                    {
                        var mergedTable = new TableSymbol(tableName, mergedColumns, docstring ?? existingTable.Description);
                        return code.Globals.AddOrUpdateDatabaseMembers(mergedTable);
                    }
                }
                else
                {
                    return code.Globals.AddOrUpdateDatabaseMembers(newTable);
                }
            }

            return code.Globals;
        }

        private static GlobalState ApplyAlterTableDocString(KustoCode code, CustomCommand commandRoot)
        {
            var tableName = commandRoot.GetFirstDescendant<NameReference>(cn => cn.NameInParent == "TableName")?.SimpleName;
            var docString = commandRoot.GetFirstDescendant<LiteralExpression>(lit => lit.NameInParent == "Documentation")?.LiteralValue as string;

            if (tableName != null
                && docString != null
                && code.Globals.Database.GetTable(tableName) is TableSymbol ts)
            {
                var newTable = ts.WithDescripton(docString);
                return code.Globals.AddOrUpdateDatabaseMembers(newTable);
            }

            return code.Globals;
        }

        private static GlobalState ApplyRenameTable(KustoCode code, CustomCommand commandRoot)
        {
            var existingTableNameNodes = commandRoot.GetDescendants<NameReference>(nr => nr.NameInParent == "TableName");

            var replacements = existingTableNameNodes.Select(nn =>
            {
                var newTableName = nn.Parent.GetFirstDescendant<NameDeclaration>(nd => nd.NameInParent == "NewTableName")?.SimpleName;
                if (newTableName != null
                    && code.Globals.Database.GetTable(nn.SimpleName) is TableSymbol existingTable)
                {
                    return (existingTable, existingTable.WithName(newTableName));
                }

                return default;
            })
            .Where(nt => nt != default)
            .ToList();

            return code.Globals.ReplaceDatabaseMembers(replacements);
        }

        private static GlobalState ApplyDropTable(KustoCode code, CustomCommand commandRoot)
        {
            var tables = commandRoot.GetDescendants<NameReference>()
                .Select(nr => code.Globals.Database.GetTable(nr.SimpleName))
                .Where(t => t != null)
                .ToList();
            return code.Globals.RemoveDatabaseMembers(tables);
        }

        private static GlobalState ApplySetTable(KustoCode code, CustomCommand commandRoot)
        {
            var tableName = commandRoot.GetFirstDescendant<Name>()?.SimpleName;
            var docstring = GetPropertyValueText(commandRoot, "docstring");

            if (tableName != null
                && GetInputResult(code) is TableSymbol inputSchema)
            {
                if (code.Globals.Database.GetTable(tableName) == null)
                {
                    var newTable = inputSchema.WithName(tableName).WithDescripton(docstring);
                    return code.Globals.AddOrUpdateDatabaseMembers(newTable);
                }
            }

            return code.Globals;
        }

        private static GlobalState ApplyAppendTable(KustoCode code, CustomCommand commandRoot)
        {
            var tableName = commandRoot.GetFirstDescendant<Name>()?.SimpleName;
            var docstring = GetPropertyValueText(commandRoot, "docstring");
            var extend = GetPropertyValue(commandRoot, "extend_schema", false);

            if (tableName != null
                && code.Globals.Database.GetTable(tableName) is TableSymbol existingTable)
            {
                var newTable = existingTable.WithDescripton(docstring ?? existingTable.Description);

                if (extend && GetInputResult(code) is TableSymbol inputResult)
                {
                    newTable = newTable.WithColumns(ExtendColumns(existingTable.Columns, inputResult.Columns));
                }

                return code.Globals.AddOrUpdateDatabaseMembers(newTable);
            }

            return code.Globals;
        }

        private static GlobalState ApplySetOrAppendTable(KustoCode code, CustomCommand commandRoot)
        {
            var tableName = commandRoot.GetFirstDescendant<Name>()?.SimpleName;
            var docstring = GetPropertyValueText(commandRoot, "docstring");
            var extend = GetPropertyValue(commandRoot, "extend_schema", false);

            if (tableName != null)
            {
                var existingTable = code.Globals.Database.GetTable(tableName);

                if (existingTable != null)
                {
                    var newTable = existingTable.WithDescripton(docstring ?? existingTable.Description);

                    if (extend && GetInputResult(code) is TableSymbol inputResult)
                    {
                        newTable = newTable.WithColumns(ExtendColumns(existingTable.Columns, inputResult.Columns));
                    }

                    return code.Globals.AddOrUpdateDatabaseMembers(newTable);
                }
                else if (GetInputResult(code) is TableSymbol inputResult)
                {
                    var newTable = inputResult.WithName(tableName).WithDescripton(docstring);
                    return code.Globals.AddOrUpdateDatabaseMembers(newTable);
                }
            }

            return code.Globals;
        }

        private static GlobalState ApplySetOrReplaceTable(KustoCode code, CustomCommand commandRoot)
        {
            var tableName = commandRoot.GetFirstDescendant<Name>()?.SimpleName;
            var docstring = GetPropertyValueText(commandRoot, "docstring");
            var extend = GetPropertyValue(commandRoot, "extend_schema", false);
            var recreate = GetPropertyValue(commandRoot, "recreate_schema", false);

            if (tableName != null)
            {
                var existingTable = code.Globals.Database.GetTable(tableName);

                if (existingTable != null)
                {
                    var newTable = existingTable.WithDescripton(docstring ?? existingTable.Description);
                    var inputResult = GetInputResult(code) as TableSymbol;

                    if (recreate && inputResult != null)
                    {
                        newTable = newTable.WithColumns(inputResult.Columns);
                    }
                    else if (extend && inputResult != null)
                    {
                        newTable = newTable.WithColumns(ExtendColumns(existingTable.Columns, inputResult.Columns));
                    }

                    return code.Globals.AddOrUpdateDatabaseMembers(newTable);
                }
                else if (GetInputResult(code) is TableSymbol inputResult)
                {
                    var newTable = inputResult.WithName(tableName).WithDescripton(docstring);
                    return code.Globals.AddOrUpdateDatabaseMembers(newTable);
                }
            }

            return code.Globals;
        }

        private static GlobalState ApplyAlterColumnType(KustoCode code, CustomCommand commandRoot)
        {
            var columnNameNode = commandRoot.GetFirstDescendant<Expression>(e => e.NameInParent == "ColumnName");
            var columnTypeNode = commandRoot.GetFirstDescendant<TypeExpression>(e => e.NameInParent == "ColumnType");

            if (columnNameNode != null
                && columnTypeNode != null
                && TryGetDatabaseTableAndColumn(code.Globals, columnNameNode, out var database, out var table, out var column)
                && GetColumnType(columnTypeNode) is ScalarSymbol columnType)
            {
                var newColumn = column!.WithType(columnType);
                var newTable = table!.AddOrUpdateColumns(newColumn);
                var newDatabase = database!.AddOrUpdateMembers(newTable);
                return code.Globals.AddOrUpdateClusterDatabase(newDatabase!);
            }

            return code.Globals;
        }

        private static GlobalState ApplyDropColumn(KustoCode code, CustomCommand commandRoot)
        {
            var columnNameNode = commandRoot.GetFirstDescendant<Expression>(e => e.NameInParent == "ColumnName");
            if (columnNameNode != null
                && TryGetDatabaseTableAndColumn(code.Globals, columnNameNode, out var database, out var table, out var column))
            {
                var newTable = table!.RemoveColumns(column!);
                var newDb = database!.AddOrUpdateMembers(newTable);
                return code.Globals.AddOrUpdateClusterDatabase(newDb);
            }

            return code.Globals;
        }

        private static GlobalState ApplyDropTableColumns(KustoCode code, CustomCommand commandRoot)
        {
            var tableName = commandRoot.GetFirstDescendant<NameReference>(n => n.NameInParent == "TableName")?.SimpleName;

            if (code.Globals.Database.GetTable(tableName) is TableSymbol table)
            {
                var columns = commandRoot
                    .GetDescendants<NameReference>()
                    .Skip(1) // skip the table name
                    .Select(c => table.GetColumn(c.SimpleName))
                    .Where(c => c != null)
                    .ToList();

                var newTable = table.RemoveColumns(columns);
                return code.Globals.AddOrUpdateDatabaseMembers(newTable);
            }

            return code.Globals;
        }

        private static GlobalState ApplyRenameColumn(KustoCode code, CustomCommand commandRoot)
        {
            var columnNameNode = commandRoot.GetFirstDescendant<Expression>(e => e.NameInParent == "ColumnName");
            var newColumnName = commandRoot.GetFirstDescendant<NameDeclaration>(e => e.NameInParent == "NewColumnName")?.SimpleName;

            if (newColumnName != null
                && columnNameNode != null
                && TryGetDatabaseTableAndColumn(code.Globals, columnNameNode, out var database, out var table, out var column))
            {
                var newColumn = column!.WithName(newColumnName);
                var newTable = table!.ReplaceColumns((column, newColumn));
                var newDb = database!.AddOrUpdateMembers(newTable);
                return code.Globals.AddOrUpdateClusterDatabase(newDb);
            }

            return code.Globals;
        }

        private static GlobalState ApplyRenameColumns(KustoCode code, CustomCommand commandRoot)
        {
            var columnNameReplacements = commandRoot
                .GetDescendants<NameDeclaration>(nd => nd.NameInParent == "NewColumnName")
                .Select(nd =>
                {
                    var equalToken = nd.GetLastToken().GetNextToken();
                    if (equalToken != null
                        && equalToken.Kind == SyntaxKind.EqualToken
                        && GetNextPeer(equalToken) is Expression originalNameNode
                        && TryGetDatabaseTableAndColumn(code.Globals, originalNameNode,
                            out var database, out var table, out var column))
                    {
                        return (database, table, column, newName: nd.SimpleName);
                    }
                    else
                    {
                        return default;
                    }
                })
                .Where(x => x.column != null)
                .ToList();

            var newDbs = columnNameReplacements.GroupBy(x => x.database!)
                .Select(dbg => 
                    dbg.Key.AddOrUpdateMembers(
                        dbg
                        .GroupBy(x => x.table!)
                        .Select(tg =>
                            tg.Key.ReplaceColumns(
                                tg.Select(x => (x.column!, x.column!.WithName(x.newName!))).ToArray()))))
                .ToList();

            return code.Globals.AddOrUpdateClusterDatabases(newDbs);
        }

        private static GlobalState ApplyAlterTableColumnDocStrings(KustoCode code, CustomCommand commandRoot)
        {
            var tableName = commandRoot.GetFirstDescendant<NameReference>(nr => nr.NameInParent == "TableName")?.SimpleName;
            if (tableName != null && code.Globals.Database.GetTable(tableName) is TableSymbol table)
            {
                var newColumnMap = commandRoot.GetDescendants<NameReference>(nr => nr.NameInParent == "ColumnName")
                    .Select(cn =>
                    {
                        var columnName = cn.SimpleName;
                        if (table.Columns.FirstOrDefault(c => c.Name == columnName) is ColumnSymbol column)
                        {
                            var colon = cn.GetLastToken().GetNextToken();
                            if (colon != null && colon.Kind == SyntaxKind.ColonToken)
                            {
                                var docstring = GetLiteralValueAfterElement(colon);
                                if (docstring != null)
                                    return column.WithDescription(docstring);
                            }
                        }

                        return null;
                    })
                    .Where(x => x != null)
                    .ToDictionary(x => x!.Name);

                var newColumns = table.Columns
                    .Select(c => newColumnMap.TryGetValue(c.Name, out var newCol) ? newCol : c.WithDescription(""))
                    .ToList();

                var newTable = table.AddOrUpdateColumns(newColumns!);
                return code.Globals.AddOrUpdateDatabaseMembers(newTable);
            }

            return code.Globals;
        }

        private static GlobalState ApplyAlterMergeTableColumnDocStrings(KustoCode code, CustomCommand commandRoot)
        {
            var tableName = commandRoot.GetFirstDescendant<NameReference>(nr => nr.NameInParent == "TableName")?.SimpleName;
            if (tableName != null && code.Globals.Database.GetTable(tableName) is TableSymbol table)
            {
                var newColumns = commandRoot.GetDescendants<NameReference>(nr => nr.NameInParent == "ColumnName")
                    .Select(cn =>
                    {
                        var columnName = cn.SimpleName;
                        if (table.Columns.FirstOrDefault(c => c.Name == columnName) is ColumnSymbol column)
                        {
                            var colon = cn.GetLastToken().GetNextToken();
                            if (colon != null && colon.Kind == SyntaxKind.ColonToken)
                            {
                                var docstring = GetLiteralValueAfterElement(colon);
                                if (docstring != null)
                                    return column.WithDescription(docstring);
                            }
                        }

                        return null;
                    })
                    .Where(x => x != null)
                    .ToList();

                var newTable = table.AddOrUpdateColumns(newColumns!);

                return code.Globals.AddOrUpdateDatabaseMembers(newTable);
            }

            return code.Globals;
        }

        private static GlobalState ApplyCreateExternalTable(KustoCode code, CustomCommand commandRoot)
        {
            var nn = commandRoot.GetFirstDescendant<NameDeclaration>(nn => nn.NameInParent == "ExternalTableName");
            if (nn != null && code.Globals.Database.GetExternalTable(nn.SimpleName) == null)
            {
                var name = nn.SimpleName;
                var schema = GetSchemaAfterElement(code, nn);
                if (schema != null)
                {
                    var docstring = GetPropertyValueText(commandRoot, "docstring");
                    var newExternalTable = new ExternalTableSymbol(name, schema, docstring);
                    return code.Globals.AddOrUpdateDatabaseMembers(newExternalTable);
                }
            }

            return code.Globals;
        }

        private static GlobalState ApplyAlterExternalTable(KustoCode code, CustomCommand commandRoot)
        {
            var nn = commandRoot.GetFirstDescendant<NameDeclaration>(nn => nn.NameInParent == "ExternalTableName");
            if (nn != null && code.Globals.Database.GetExternalTable(nn.SimpleName) is ExternalTableSymbol et)
            {
                var name = nn.SimpleName;
                var schema = GetSchemaAfterElement(code, nn);
                if (schema != null)
                {
                    var docstring = GetPropertyValueText(commandRoot, "docstring");
                    var newExternalTable = new ExternalTableSymbol(name, schema, docstring);
                    return code.Globals.AddOrUpdateDatabaseMembers(newExternalTable);
                }
            }

            return code.Globals;
        }

        private static GlobalState ApplyCreateOrAlterExternalTable(KustoCode code, CustomCommand commandRoot)
        {
            var nn = commandRoot.GetFirstDescendant<NameDeclaration>(nn => nn.NameInParent == "ExternalTableName");
            if (nn != null)
            {
                var name = nn.SimpleName;
                var schema = GetSchemaAfterElement(code, nn);
                if (schema != null)
                {
                    var docstring = GetPropertyValueText(commandRoot, "docstring");
                    var newExternalTable = new ExternalTableSymbol(name, schema, docstring);
                    return code.Globals.AddOrUpdateDatabaseMembers(newExternalTable);
                }
            }

            return code.Globals;
        }

        private static GlobalState ApplyDropExternalTable(KustoCode code, CustomCommand commandRoot)
        {
            var nn = commandRoot.GetFirstDescendant<NameReference>(nn => nn.NameInParent == "ExternalTableName");
            if (nn != null && code.Globals.Database.GetExternalTable(nn.SimpleName) is ExternalTableSymbol et)
            {
                return code.Globals.RemoveDatabaseMembers(et);
            }

            return code.Globals;
        }

        private static GlobalState ApplyCreateMaterializedView(KustoCode code, CustomCommand commandRoot)
        {
            var nn = commandRoot.GetFirstDescendant<NameDeclaration>(nn =>
                (PreviousTokenIs(nn, "materialized-view") || PreviousTokenIs(nn, ")")) && NextTokenIs(nn, "on"));

            if (nn != null && code.Globals.Database.GetMaterializedView(nn.SimpleName) == null)
            {
                var name = nn.SimpleName;
                var docstring = GetPropertyValueText(commandRoot, "docstring");
                var body = commandRoot.GetFirstDescendant<FunctionBody>();
                var schema = GetMaterializedViewResult(code);

                if (schema is TableSymbol table && body != null)
                {
                    var newView = new MaterializedViewSymbol(name, table.Columns, body.ToString(), docstring);
                    return code.Globals.AddOrUpdateDatabaseMembers(newView);
                }
            }

            return code.Globals;
        }

        private static GlobalState ApplyAlterMaterializedView(KustoCode code, CustomCommand commandRoot)
        {
            var nn = commandRoot.GetFirstDescendant<NameReference>(nn =>
                (PreviousTokenIs(nn, "materialized-view") || PreviousTokenIs(nn, ")")) && NextTokenIs(nn, "on"));

            if (nn != null && code.Globals.Database.GetMaterializedView(nn.SimpleName) != null)
            {
                var name = nn.SimpleName;
                var docstring = GetPropertyValueText(commandRoot, "docstring");
                var body = commandRoot.GetFirstDescendant<FunctionBody>();
                var schema = GetMaterializedViewResult(code);

                if (schema is TableSymbol table && body != null)
                {
                    var newView = new MaterializedViewSymbol(name, table.Columns, body.ToString(), docstring);
                    return code.Globals.AddOrUpdateDatabaseMembers(newView);
                }
            }

            return code.Globals;
        }

        private static GlobalState ApplyCreateOrAlterMaterializedView(KustoCode code, CustomCommand commandRoot)
        {
            var nn = commandRoot.GetFirstDescendant<NameReference>(nn =>
                (PreviousTokenIs(nn, "materialized-view") || PreviousTokenIs(nn, ")")) && NextTokenIs(nn, "on"));

            if (nn != null)
            {
                var name = nn.SimpleName;
                var docstring = GetPropertyValueText(commandRoot, "docstring");
                var body = commandRoot.GetFirstDescendant<FunctionBody>();
                var schema = GetMaterializedViewResult(code);

                if (schema is TableSymbol table && body != null)
                {
                    var newView = new MaterializedViewSymbol(name, table.Columns, body.ToString(), docstring);
                    return code.Globals.AddOrUpdateDatabaseMembers(newView);
                }
            }

            return code.Globals;
        }

        private static GlobalState ApplyAlterMaterializedViewDocString(KustoCode code, CustomCommand commandRoot)
        {
            var name = GetNameAfterToken(commandRoot, "materialized-view");
            var docstring = GetLiteralValueAfterToken(commandRoot, "docstring");
            if (name != null && docstring != null
                && code.Globals.Database.GetMaterializedView(name) is MaterializedViewSymbol existingView)
            {
                var newView = existingView.WithDescripton(docstring);
                return code.Globals.AddOrUpdateDatabaseMembers(newView);
            }

            return code.Globals;
        }

        private static GlobalState ApplyDropMaterializedView(KustoCode code, CustomCommand commandRoot)
        {
            var name = GetNameAfterToken(commandRoot, "materialized-view");
           
            if (name != null && code.Globals.Database.GetMaterializedView(name) is MaterializedViewSymbol existingView)
            {
                return code.Globals.RemoveDatabaseMembers(existingView);
            }

            return code.Globals;
        }

        private static GlobalState ApplyRenameMaterializedView(KustoCode code, CustomCommand commandRoot)
        {
            var name = GetNameAfterToken(commandRoot, "materialized-view");
            var newName = GetNameAfterToken(commandRoot, "to");
            if (name != null && newName != null
                && code.Globals.Database.GetMaterializedView(name) is MaterializedViewSymbol existingView)
            {
                var newView = existingView.WithName(newName);
                return code.Globals.ReplaceDatabaseMembers((existingView, newView));
            }

            return code.Globals;
        }

        private static GlobalState ApplyExecuteDatabaseScript(KustoCode code, CustomCommand commandRoot)
        {
            var otherCommands = commandRoot.GetDescendants<CustomCommand>().Select(cc => cc.ToString());
            var globals = code.Globals;

            foreach (var otherCommand in otherCommands)
            {
                globals = globals.ApplyCommand(otherCommand);
            }

            return globals;
        }

        private static IReadOnlyList<ColumnSymbol> MergeColumns(IReadOnlyList<ColumnSymbol> existingColumns, IReadOnlyList<ColumnSymbol> newColumns)
        {
            var nameToExistingColumnMap = existingColumns.ToDictionary(c => c.Name);
            var newList = existingColumns.ToList();

            foreach (var newColumn in newColumns)
            {
                if (nameToExistingColumnMap.TryGetValue(newColumn.Name, out var existingColumn))
                {
                    // if any new column has same name but different type then command fails
                    if (existingColumn.Type != newColumn.Type)
                        return existingColumns;

                    // do nothing, column already exists
                }
                else
                {
                    newList.Add(newColumn);
                }
            }

            return newList;
        }

        private static IReadOnlyList<ColumnSymbol> ExtendColumns(IReadOnlyList<ColumnSymbol> existingColumns, IReadOnlyList<ColumnSymbol> newColumns)
        {
            if (newColumns.Count > existingColumns.Count)
            {
                var newList = existingColumns.ToList();
                for (int i = existingColumns.Count; i < newColumns.Count; i++)
                {
                    newList.Add(newColumns[i]);
                }

                return newList;
            }

            return existingColumns;
        }

        private static bool TryGetDatabaseTableAndColumn(
            GlobalState globals, Expression expr,
            out DatabaseSymbol? database,
            out TableSymbol? table,
            out ColumnSymbol? column)
        {
            if (expr is PathExpression pe)
            {
                if (pe.Expression is PathExpression dbPath
                    && dbPath.Expression is NameReference dbRef
                    && dbPath.Selector is NameReference dbTableRef
                    && pe.Selector is NameReference dbTableColumnRef)
                {
                    database = globals.Cluster.GetDatabase(dbRef.SimpleName);
                    table = database?.GetTable(dbTableRef.SimpleName);
                    column = table?.GetColumn(dbTableColumnRef.SimpleName);
                    return database != null && table != null && column != null;
                }
                else if (pe.Expression is NameReference tableRef
                    && pe.Selector is NameReference columnRef)
                {
                    database = globals.Database;
                    table = database.GetTable(tableRef.SimpleName);
                    column = table?.GetColumn(columnRef.SimpleName);
                    return database != null && table != null && column != null;
                }
            }

            database = null;
            table = null;
            column = null;
            return false;
        }

        private static ScalarSymbol? GetColumnType(TypeExpression tex)
        {
            return tex is PrimitiveTypeExpression pex
                ? ScalarTypes.GetSymbol(pex.Type.Text)
                : null;
        }

        private static string? GetSchemaAfterElement(KustoCode code, SyntaxElement? element)
        {
            var openParen = element?.GetLastToken()?.GetNextToken();
            if (openParen != null && openParen.Kind == SyntaxKind.OpenParenToken)
            {
                var closeParen = GetMatchingCloseParen(openParen);
                if (closeParen != null)
                {
                    return code.Text.Substring(openParen.TextStart, closeParen.End - openParen.TextStart);
                }
            }

            return null;
        }

        private static string? GetNameAfterToken(SyntaxElement root, string tokenText)
        {
            var token = root.GetFirstDescendant<SyntaxToken>(tk => tk.Text == tokenText);
            return GetNameAfterElement(token);
        }

        private static string? GetNameAfterElement(SyntaxElement element)
        {
            var next = GetNextPeer(element);
            if (next is NameReference nr)
                return nr.SimpleName;
            else if (next is NameDeclaration nd)
                return nd.SimpleName;
            return null;
        }

        private static string? GetLiteralValueAfterToken(SyntaxElement root, string tokenText)
        {
            var token = root.GetFirstDescendant<SyntaxToken>(tk => tk.Text == tokenText);
            return GetLiteralValueAfterElement(token);
        }

        private static string? GetLiteralValueAfterElement(SyntaxElement element)
        {
            var next = GetNextPeer(element);
            if (next is LiteralExpression lit)
                return lit.LiteralValue.ToString();
            return null;
        }

        private static string? GetPropertyValueText(SyntaxElement root, string propertyName)
        {
            var value = GetPropertyValue(root, propertyName);
            return value?.ToString();
        }

        private static TValue GetPropertyValue<TValue>(SyntaxElement root, string propertyName, TValue defaultValue)
        {
            var value = GetPropertyValue(root, propertyName);
            if (value != null)
            {
                return (TValue)Convert.ChangeType(value, typeof(TValue));
            }

            return defaultValue;
        }

        private static object? GetPropertyValue(SyntaxElement root, string propertyName)
        {
            var propertyNameNode = root.GetFirstDescendant<SyntaxElement>(e =>
                e.NameInParent == "PropertyName"
                && (e is SyntaxToken tk && tk.Text == propertyName
                    || e is NameDeclaration nd && nd.SimpleName == propertyName)
                && e.GetNextSibling() is SyntaxToken eq && eq.Kind == SyntaxKind.EqualToken);

            var valueNode = propertyNameNode?.GetNextSibling()?.GetNextSibling();
            if (valueNode is SyntaxToken valueToken)
                return valueToken.Value;
            else if (valueNode is LiteralExpression lit)
                return lit.LiteralValue;
            return null;
        }

        private static SyntaxToken? GetMatchingCloseParen(SyntaxToken openParen)
        {
            var nextToken = openParen.GetNextToken();
            while (nextToken != null)
            {
                if (nextToken.Kind == SyntaxKind.CloseParenToken)
                    return nextToken;

                if (nextToken.Kind == SyntaxKind.OpenParenToken)
                {
                    nextToken = GetMatchingCloseParen(nextToken);
                    continue;
                }

                nextToken = nextToken.GetNextToken();
            }

            return null;
        }

        private static Symbol? GetInputResult(KustoCode code)
        {
            var analyzedCode = code.HasSemantics
                ? code
                : code.Analyze();

            var commandRoot = analyzedCode.Syntax.GetFirstDescendant<CustomCommand>();
            if (commandRoot == null)
                return null;

            var pipeToken = commandRoot.GetFirstDescendant<SyntaxToken>(tk => tk.Text == "<|");
            if (pipeToken == null)
                return null;

            var input = GetNextPeer(pipeToken);
            if (input == null)
                return null;

            if (input is SyntaxList<SeparatedElement<Statement>> statementList)
            {
                for (int i = statementList.Count - 1; i >= 0; i--)
                {
                    if (statementList[i].Element is ExpressionStatement es)
                    {
                        return es.Expression.ResultType;
                    }
                }
            }
            else if (input is Expression ex)
            {
                return ex.ResultType;
            }

            return null;
        }

        private static Symbol? GetMaterializedViewResult(KustoCode code)
        {
            var analyzedCode = code.HasSemantics
                ? code
                : code.Analyze();

            var commandRoot = analyzedCode.Syntax.GetFirstDescendant<CustomCommand>();
            if (commandRoot == null)
                return null;

            var body = commandRoot.GetFirstDescendant<FunctionBody>();
            if (body == null)
                return null;

            return body.Expression?.ResultType;
        }

        private static bool PreviousTokenIs(SyntaxElement element, string tokenText)
        {
            var firstToken = element is SyntaxToken tk ? tk : element.GetFirstToken();
            var prevToken = firstToken.GetPreviousToken();
            return prevToken != null && prevToken.Text == tokenText;
        }

        private static bool NextTokenIs(SyntaxElement element, string tokenText)
        {
            var lastToken = element is SyntaxToken tk ? tk : element.GetLastToken();
            var nextToken = lastToken.GetNextToken();
            return nextToken != null && nextToken.Text == tokenText;
        }

        private static SyntaxElement? GetNextPeer(SyntaxElement element)
        {
            var lastToken = element is SyntaxToken elementToken ? elementToken : element.GetLastToken();
            var nextToken = lastToken?.GetNextToken();

            if (nextToken != null)
            {
                var parent = element.Parent;
                while (parent != null && !parent.IsAncestorOf(nextToken))
                {
                    parent = parent.Parent;
                }

                if (parent != null)
                {
                    return nextToken.GetFirstAncestor<SyntaxElement>(e => e.Parent == parent);
                }
            }

            return null;
        }
    }
}
