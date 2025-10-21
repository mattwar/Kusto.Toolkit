using Kusto.Language;
using Kusto.Language.Editor;
using Kusto.Language.Symbols;
using Kusto.Language.Syntax;
using Kusto.Language.Utils;
using Microsoft.Identity.Client.NativeInterop;
using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;

namespace Kusto.Toolkit
{

    public enum ApplyKind
    {
        /// <summary>
        /// All failures reported
        /// </summary>
        Strict,

        /// <summary>
        /// Skip all commands do not modified schema (cannot be applied)
        /// </summary>
        SkipUnhandled, 

        /// <summary>
        /// Skip all apply failures
        /// </summary>
        SkipFailures
    }

    public static partial class GlobalStateExtensions
    {
        /// <summary>
        /// Returns a new <see cref="GlobalState"/> modified by the commands in order.
        /// The default cluster and database must already be set or no nothing will be applied.
        /// </summary>
        public static ApplyCommandResult ApplyCommands(this GlobalState globals, IEnumerable<string> commands, ApplyKind kind = ApplyKind.Strict)
        {
            ApplyCommandResult result = null!;

            foreach (var command in commands)
            {
                result = globals.ApplyCommand(command, kind);
                if (!result.Succeeded)
                    return result;
                globals = result.Globals;
            }

            return result;
        }

        /// <summary>
        /// Returns a new <see cref="GlobalState"/> modified by the commands in order.
        /// The default cluster and database must already be set or no nothing will be applied.
        /// </summary>
        public static ApplyCommandResult ApplyCommands(this GlobalState globals, params string[] commands)
        {
            return globals.ApplyCommands((IEnumerable<string>)commands);
        }

        /// <summary>
        /// Returns a new <see cref="GlobalState"/> modified by the command.
        /// The default cluster and database must already be set or no nothing will be applied.
        /// </summary>
        public static ApplyCommandResult ApplyCommand(this GlobalState globals, string command, ApplyKind kind = ApplyKind.Strict)
        {
            var result = ApplyCommandInternal(globals, command, kind);

            if (!result.Succeeded && kind == ApplyKind.SkipFailures)
                return new ApplyCommandResult(globals);

            return result;
        }

        private static ApplyCommandResult ApplyCommandInternal(GlobalState globals, string command, ApplyKind kind)
        {
            // if is not a command
            if (KustoCode.GetKind(command) != CodeKinds.Command)
                return new ApplyCommandResult(globals, command, Diagnostics.GetCommandTextIsNotCommand());
 
            // don't analyze to avoid failures on commands that incorrectly use name-references for declarations
            var code = KustoCode.Parse(command, globals);

            var errors = GetErrors(code.GetDiagnostics());

            // we have syntax errors
            if (errors.Count > 0)
                return GetErrorResult(code, errors);

            var commandRoot = code.Syntax.GetFirstDescendant<CustomCommand>();

            // we do not have a recognizable command.. it may not be a legal command or may be only partial
            if (commandRoot == null)
                return new ApplyCommandResult(globals, command, Diagnostics.GetCommandNotRecognized());

            // handle all known schema altering commands that affect globals
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
                nameof(EngineCommands.DropFunction) =>
                    ApplyDropFunction(code, commandRoot),
                nameof(EngineCommands.DropFunctions) =>
                    ApplyDropFunctions(code, commandRoot),

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
                nameof(EngineCommands.DropTable) =>
                    ApplyDropTable(code, commandRoot),
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
                    ApplyAlterTableColumnDocStrings(code, commandRoot, isMerge: false),
                nameof(EngineCommands.AlterMergeTableColumnDocStrings) =>
                    ApplyAlterTableColumnDocStrings(code, commandRoot, isMerge: true),

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

                _ => kind == ApplyKind.SkipUnhandled
                        ? new ApplyCommandResult(code.Globals)
                        : new ApplyCommandResult(code.Globals, code.Text, Diagnostics.GetUnhandledCommandKind(commandRoot.CommandKind))
            };
        }

        private static IReadOnlyList<Diagnostic> GetErrors(IReadOnlyList<Diagnostic> diagnostics)
        {
            if (diagnostics.Count > 0 && diagnostics.Any(d => d.Severity == DiagnosticSeverity.Error))
            {
                if (diagnostics.All(d => d.Severity == DiagnosticSeverity.Error))
                    return diagnostics;
                return diagnostics.Where(d => d.Severity == DiagnosticSeverity.Error).ToReadOnly();
            }

            return Array.Empty<Diagnostic>();
        }

        private static ApplyCommandResult GetErrorResult(KustoCode code, IReadOnlyList<Diagnostic> errors)
        {
            return new ApplyCommandResult(code.Globals, code.Text, new[] { Diagnostics.GetCommandHasErrors() }.Concat(errors).ToArray());
        }

        private static bool HasIfNotExists(CustomCommand commandRoot)
        {
            return commandRoot.GetFirstDescendant<SyntaxToken>(tk => tk.Text == "ifnotexists") != null;
        }

        private static bool HasIfExists(CustomCommand commandRoot)
        {
            return commandRoot.GetFirstDescendant<SyntaxToken>(tk => tk.Text == "ifexists") != null;
        }

        private static ApplyCommandResult GetMissingSyntaxResult(KustoCode code, SyntaxElement? location = null)
        {
            var dx = Diagnostics.GetCommandHasMissingElements();
            if (location != null)
                dx = dx.WithLocation(location);
            return new ApplyCommandResult(code.Globals, code.Text, dx);
        }

        private static ApplyCommandResult GetEntityDoesNotExistResult(KustoCode code, string entityKind, string entityName, SyntaxElement? location = null)
        {
            var dx = Diagnostics.GetEntityDoesNotExist(entityKind, entityName);
            if (location != null)
                dx = dx.WithLocation(location);
            return new ApplyCommandResult(code.Globals, code.Text, dx);
        }

        public static ApplyCommandResult GetEntityAlreadyExistsResult(KustoCode code, string entityKind, string entityName, SyntaxElement? location = null)
        {
            var dx = Diagnostics.GetEntityAlreadyExists(entityKind, entityName);
            if (location != null)
                dx = dx.WithLocation(location);
            return new ApplyCommandResult(code.Globals, code.Text, dx);
        }

        #region Functions
        private static ApplyCommandResult ApplyCreateFunction(KustoCode code, CustomCommand commandRoot)
        {
            if (GetNameWithTag(commandRoot, "FunctionName") is { } name
                && commandRoot.GetFirstDescendant<FunctionParameters>()?.ToString() is { } parameters
                && commandRoot.GetFirstDescendant<FunctionBody>()?.ToString() is { } body
                && GetPropertyValueText(commandRoot, "docstring") is var docstring)
            {
                if (code.Globals.Database.GetFunction(name) is null)
                {
                    var newFunction = new FunctionSymbol(name, parameters, body, docstring);
                    var newGlobals = code.Globals.AddOrUpdateDatabaseMembers(newFunction);
                    return new ApplyCommandResult(newGlobals);
                }
                else if (HasIfNotExists(commandRoot))
                {
                    // succeed with no change
                    return new ApplyCommandResult(code.Globals);
                }
                else
                {
                    // ifnotexists not specified so this is an error
                    return GetEntityAlreadyExistsResult(code, "function", name);
                }
            }
            else
            {
                return GetMissingSyntaxResult(code);
            }
        }

        private static ApplyCommandResult ApplyAlterFunction(KustoCode code, CustomCommand commandRoot)
        {
            if (GetNameWithTag(commandRoot, "FunctionName") is string name
                && commandRoot.GetFirstDescendant<FunctionParameters>()?.ToString() is { } parameters
                && commandRoot.GetFirstDescendant<FunctionBody>()?.ToString() is { } body
                && GetPropertyValueText(commandRoot, "docstring") is var docstring)
            {
                // if function already exists, alter it
                if (code.Globals.Database.GetFunction(name) != null)
                {
                    var newFunction = new FunctionSymbol(name, parameters, body, docstring);
                    var newGlobals = code.Globals.AddOrUpdateDatabaseMembers(newFunction);
                    return new ApplyCommandResult(newGlobals);
                }
                else
                {
                    // function does not exists, so error
                    return GetEntityDoesNotExistResult(code, "function", name);
                }
            }
            else
            {
                return GetMissingSyntaxResult(code);
            }
        }

        private static ApplyCommandResult ApplyDropFunction(KustoCode code, CustomCommand commandRoot)
        {
            if (GetNameWithTag(commandRoot, "FunctionName") is string name)
            {
                if (code.Globals.Database.GetFunction(name) is { } fn)
                {
                    var newGlobals = code.Globals.RemoveDatabaseMembers(fn);
                    return new ApplyCommandResult(newGlobals);
                }
                else if (HasIfExists(commandRoot))
                {
                    // succeed with no change
                    return new ApplyCommandResult(code.Globals);
                }
                else
                {
                    // error because function does not exist
                    return GetEntityDoesNotExistResult(code, "function", name);
                }
            }
            else
            {
                return GetMissingSyntaxResult(code);
            }
        }

        private static ApplyCommandResult ApplyDropFunctions(KustoCode code, CustomCommand commandRoot)
        {
            if (GetNamesWithTag(commandRoot, "FunctionName") is { } names
                && names.Count > 0)
            {
                var fns = names.Select(n => code.Globals.Database.GetFunction(n)).Where(f => f != null).ToList();
                if (fns.Count == names.Count || HasIfExists(commandRoot))
                {
                    var newGlobals = code.Globals.RemoveDatabaseMembers(fns);
                    return new ApplyCommandResult(newGlobals);
                }
                else
                {
                    // error because function does not exist
                    var missingNames = names.Except(fns.Select(f => f.Name)).ToList();
                    return GetEntityDoesNotExistResult(code, "function", missingNames[0]);
                }
            }
            else

            {
                return GetMissingSyntaxResult(code);
            }
        }

        private static ApplyCommandResult ApplyCreateOrAlterFunction(KustoCode code, CustomCommand commandRoot)
        {
            if (GetNameWithTag(commandRoot, "FunctionName") is string name
                && commandRoot.GetFirstDescendant<FunctionParameters>()?.ToString() is { } parameters
                && commandRoot.GetFirstDescendant<FunctionBody>()?.ToString() is { } body
                && GetPropertyValueText(commandRoot, "docstring") is var docstring)
            {
                var newFunction = new FunctionSymbol(name, parameters, body, docstring);
                return new ApplyCommandResult(code.Globals.AddOrUpdateDatabaseMembers(newFunction));
            }
            else
            {
                return GetMissingSyntaxResult(code);
            }
        }

        private static ApplyCommandResult ApplyAlterFunctionDocString(KustoCode code, CustomCommand commandRoot)
        {
            if (GetNameAfterToken(commandRoot, "function") is string name
                && GetLiteralValueAfterToken(commandRoot, "docstring") is string description)
            {
                // function exists?
                if (code.Globals.Database.GetFunction(name) is { } fn)
                {
                    var newFn = fn.WithDescription(description);
                    var newGlobals = code.Globals.AddOrUpdateDatabaseMembers(newFn);
                    return new ApplyCommandResult(newGlobals);
                }
                else
                {
                    return GetEntityDoesNotExistResult(code, "function", name);
                }
            }
            else
            {
                return GetMissingSyntaxResult(code);
            }
        }
        #endregion

        #region Tables
        private static ApplyCommandResult ApplyCreateTable(KustoCode code, CustomCommand commandRoot)
        {
            if (GetElementsWithTag(commandRoot, "TableName") is { } nameElements
                && GetPropertyValueText(commandRoot, "docstring") is var docstring)
            {
                var newTables = new List<TableSymbol>();
                foreach (var nn in nameElements)
                {
                    if (GetName(nn) is string name
                        && GetSchemaAfterElement(code, nn) is { } schema)
                    {
                        if (code.Globals.Database.GetTable(name) == null)
                        {
                            newTables.Add(new TableSymbol(name, schema, docstring));
                        }
                        else
                        {
                            // table already exists
                            return GetEntityAlreadyExistsResult(code, "table", name, nn);
                        }
                    }
                    else
                    {
                        // missing schema
                        return GetMissingSyntaxResult(code, nn);
                    }
                }

                var newGlobals = code.Globals.AddOrUpdateDatabaseMembers(newTables);
                return new ApplyCommandResult(newGlobals);
            }
            else
            {
                return GetMissingSyntaxResult(code);
            }
        }

        private static ApplyCommandResult ApplyCreateMergeTable(KustoCode code, CustomCommand commandRoot)
        {
            if (GetElementsWithTag(commandRoot, "TableName") is { } nameElements
                && GetPropertyValueText(commandRoot, "docstring") is var docstring)
            {
                var newTables = new List<TableSymbol>();
                foreach (var nn in nameElements)
                {
                    var name = GetName(nn);
                    var schema = GetSchemaAfterElement(code, nn);
                    if (schema == null)
                    {
                        // missing schema
                        return GetMissingSyntaxResult(code, nn);
                    }

                    var newTable = new TableSymbol(name, schema, docstring);

                    if (code.Globals.Database.GetTable(name) is { } existingTable)
                    {
                        // merge with existing table
                        var mergedColumns = MergeColumns(existingTable.Columns, newTable.Columns);
                        if (mergedColumns != existingTable.Columns)
                        {
                            docstring = docstring ?? existingTable.Description;
                            newTable = new TableSymbol(name, mergedColumns, docstring);
                        }
                        else
                        {
                            return new ApplyCommandResult(code.Globals, code.Text, Diagnostics.GetMergeFailed());
                        }
                    }

                    newTables.Add(newTable);
                }

                var newGlobals = code.Globals.AddOrUpdateDatabaseMembers(newTables);
                return new ApplyCommandResult(newGlobals);
            }
            else
            {
                return GetMissingSyntaxResult(code);
            }
        }

        private static ApplyCommandResult ApplyCreateTableBaseOnAnotherTable(KustoCode code, CustomCommand commandRoot)
        {
            if (GetNameWithTag(commandRoot, "TableName") is { } tableName
                && GetNameWithTag(commandRoot, "NewTableName") is { } newTableName)
            {
                if (code.Globals.Database.GetTable(tableName) is { } existingTableSymbol)
                {
                    if (code.Globals.Database.GetTable(newTableName) == null)
                    {
                        var newTableSymbol = existingTableSymbol.WithName(newTableName);
                        var newGlobals = code.Globals.AddOrUpdateDatabaseMembers(newTableSymbol);
                        return new ApplyCommandResult(newGlobals);
                    }
                    else if (HasIfNotExists(commandRoot))
                    {
                        // target table already exists but ifnotexists was specified
                        return new ApplyCommandResult(code.Globals);
                    }
                    else
                    {
                        return GetEntityAlreadyExistsResult(code, "table", newTableName);
                    }
                }
                else
                {
                    return GetEntityDoesNotExistResult(code, "table", tableName);
                }
            }
            else
            {
                return GetMissingSyntaxResult(code);
            }
        }

        private static ApplyCommandResult ApplyAlterTable(KustoCode code, CustomCommand commandRoot)
        {
            if (GetElementAfterToken(commandRoot, "table") is { } tableNameNode
                && GetName(tableNameNode) is { } tableName
                && GetSchemaAfterElement(code, tableNameNode) is { } tableSchema
                && GetPropertyValueText(commandRoot, "docstring") is var docstring)
            {
                if (code.Globals.Database.GetTable(tableName) is { } existingTable)
                {
                    var newTable = new TableSymbol(tableName, tableSchema, docstring ?? existingTable.Description);
                    return new ApplyCommandResult(code.Globals.AddOrUpdateDatabaseMembers(newTable));
                }
                else
                {
                    return GetEntityDoesNotExistResult(code, "table", tableName, tableNameNode);
                }
            }
            else
            {
                return GetMissingSyntaxResult(code);
            }
        }

        private static ApplyCommandResult ApplyAlterMergeTable(KustoCode code, CustomCommand commandRoot)
        {
            if (GetElementAfterToken(commandRoot, "table") is { } tableNameNode
                && GetName(tableNameNode) is { } tableName
                && GetSchemaAfterElement(code, tableNameNode) is { } tableSchema
                && GetPropertyValueText(commandRoot, "docstring") is var docstring)
            {
                var newTable = new TableSymbol(tableName, tableSchema, docstring);

                if (code.Globals.Database.GetTable(tableName) is TableSymbol existingTable)
                {
                    var mergedColumns = MergeColumns(existingTable.Columns, newTable.Columns);
                    var mergedTable = new TableSymbol(tableName, mergedColumns, docstring ?? existingTable.Description);
                    return new ApplyCommandResult(code.Globals.AddOrUpdateDatabaseMembers(mergedTable));
                }
                else
                {
                    return GetEntityDoesNotExistResult(code, "table", tableName, tableNameNode);
                }
            }
            else
            {
                return GetMissingSyntaxResult(code);
            }
        }

        private static ApplyCommandResult ApplyAlterTableDocString(KustoCode code, CustomCommand commandRoot)
        {
            if (GetNameWithTag(commandRoot, "TableName") is { } tableName
                && GetLiteralValueWithTag(commandRoot, "Documentation") is { } docstring)
            {
                if (code.Globals.Database.GetTable(tableName) is TableSymbol ts)
                {
                    var newTable = ts.WithDescripton(docstring);
                    var newGlobals = code.Globals.AddOrUpdateDatabaseMembers(newTable);
                    return new ApplyCommandResult(newGlobals);
                }
                else
                {
                    return GetEntityDoesNotExistResult(code, "table", tableName);
                }
            }
            else
            {
                return GetMissingSyntaxResult(code);
            }
        }

        private static ApplyCommandResult ApplyRenameTable(KustoCode code, CustomCommand commandRoot)
        {
            if (GetElementsWithTag(commandRoot, "TableName") is { } tableNameElements)
            {
                var replacements = new List<(TableSymbol original, TableSymbol replacement)>();

                foreach (var ne in tableNameElements)
                {
                    // get the table name for this name node and corresponding new table name
                    if (GetName(ne) is { } tableName
                        && GetNameWithTag(ne.Parent, "NewTableName") is { } newTableName)
                    {
                        if (code.Globals.Database.GetTable(tableName) is TableSymbol table)
                        {
                            if (code.Globals.Database.GetTable(newTableName) == null)
                            {
                                var newTable = table.WithName(newTableName);
                                replacements.Add((original: table, replacement: newTable));
                            }
                            else
                            {
                                return GetEntityAlreadyExistsResult(code, "table", newTableName);
                            }
                        }
                        else
                        {
                            return GetEntityDoesNotExistResult(code, "table", tableName);
                        }
                    }
                    else
                    {
                        return GetMissingSyntaxResult(code);
                    }
                }

                var newGlobals = code.Globals.ReplaceDatabaseMembers(replacements);
                return new ApplyCommandResult(newGlobals);
            }
            else
            {
                return GetMissingSyntaxResult(code);
            }
        }

        private static ApplyCommandResult ApplyDropTable(KustoCode code, CustomCommand commandRoot)
        {
            if (GetNamesWithTag(commandRoot, "TableName") is { } tableNames)
            {
                var hasIfExits = HasIfExists(commandRoot);
                var tables = new List<TableSymbol>();
                foreach (var n in tableNames)
                {
                    if (code.Globals.Database.GetTable(n) is TableSymbol table)
                    {
                        tables.Add(table);
                    }
                    else if (!hasIfExits)
                    {
                        return new ApplyCommandResult(code.Globals, code.Text, Diagnostics.GetEntityDoesNotExist("table", n));
                    }
                }
                var newGlobals = code.Globals.RemoveDatabaseMembers(tables);
                return new ApplyCommandResult(newGlobals);
            }
            else
            {
                return GetMissingSyntaxResult(code);
            }
        }

        private static ApplyCommandResult ApplySetTable(KustoCode code, CustomCommand commandRoot)
        {
            if (GetNameWithTag(commandRoot, "TableName") is { } tableName
                && GetPropertyValueText(commandRoot, "docstring") is var docstring
                && GetInputResult(code) is TableSymbol inputSchema)
            {
                if (code.Globals.Database.GetTable(tableName) == null)
                {
                    var newTable = inputSchema.WithName(tableName).WithDescripton(docstring);
                    var newGlobals = code.Globals.AddOrUpdateDatabaseMembers(newTable);
                    return new ApplyCommandResult(newGlobals);
                }
                else
                {
                    return GetEntityAlreadyExistsResult(code, "table", tableName);
                }
            }
            else
            {
                return GetMissingSyntaxResult(code);
            }
        }

        private static ApplyCommandResult ApplyAppendTable(KustoCode code, CustomCommand commandRoot)
        {
            if (GetNameAt(commandRoot, e => PreviousTokenIs(e, "append") || PreviousTokenIs(e, "async")) is string tableName
                && GetPropertyValueText(commandRoot, "docstring") is var docstring
                && GetPropertyValue(commandRoot, "extend_schema", false) is bool extendSchema
                && GetInputResult(code) is TableSymbol inputResult)
            {
                if (code.Globals.Database.GetTable(tableName) is TableSymbol existingTable)
                {
                    var newTable = existingTable.WithDescripton(docstring ?? existingTable.Description);

                    if (extendSchema)
                    {
                        newTable = newTable.WithColumns(ExtendColumns(existingTable.Columns, inputResult.Columns));
                    }

                    var newGlobals = code.Globals.AddOrUpdateDatabaseMembers(newTable);
                    return new ApplyCommandResult(newGlobals);
                }
                else
                {
                    return GetEntityDoesNotExistResult(code, "table", tableName);
                }
            }
            else
            {
                return GetMissingSyntaxResult(code);
            }
        }

        private static ApplyCommandResult ApplySetOrAppendTable(KustoCode code, CustomCommand commandRoot)
        {
            if (GetNameAt(commandRoot, e => PreviousTokenIs(e, "set-or-append") || PreviousTokenIs(e, "async")) is string tableName
                && GetPropertyValueText(commandRoot, "docstring") is var docstring
                && GetPropertyValue(commandRoot, "extend_schema", false) is bool extendSchema
                && GetInputResult(code) is TableSymbol inputResult)
            {
                if (code.Globals.Database.GetTable(tableName) is TableSymbol existingTable)
                {
                    var newTable = existingTable.WithDescripton(docstring ?? existingTable.Description);

                    if (extendSchema)
                    {
                        newTable = newTable.WithColumns(ExtendColumns(existingTable.Columns, inputResult.Columns));
                    }

                    var newGlobals = code.Globals.AddOrUpdateDatabaseMembers(newTable);
                    return new ApplyCommandResult(newGlobals);
                }
                else
                {
                    var newTable = inputResult.WithName(tableName).WithDescripton(docstring);
                    var newGlobals = code.Globals.AddOrUpdateDatabaseMembers(newTable);
                    return new ApplyCommandResult(newGlobals);
                }
            }
            else
            {
                return GetMissingSyntaxResult(code);
            }
        }

        private static ApplyCommandResult ApplySetOrReplaceTable(KustoCode code, CustomCommand commandRoot)
        {
            if (GetNameAt(commandRoot, e => PreviousTokenIs(e, "set-or-replace") || PreviousTokenIs(e, "async")) is string tableName
                && GetPropertyValueText(commandRoot, "docstring") is var docstring
                && GetPropertyValue(commandRoot, "extend_schema", false) is bool extendSchema
                && GetPropertyValue(commandRoot, "recreate_schema", false) is bool recreate
                && GetInputResult(code) is TableSymbol inputResult)
            {
                if (code.Globals.Database.GetTable(tableName) is TableSymbol existingTable)
                {
                    var newTable = existingTable.WithDescripton(docstring ?? existingTable.Description);
                    if (recreate)
                    {
                        newTable = newTable.WithColumns(inputResult.Columns);
                    }
                    else if (extendSchema && inputResult != null)
                    {
                        newTable = newTable.WithColumns(ExtendColumns(existingTable.Columns, inputResult.Columns));
                    }

                    var newGlobals = code.Globals.AddOrUpdateDatabaseMembers(newTable);
                    return new ApplyCommandResult(newGlobals);
                }
                else
                {
                    var newTable = inputResult.WithName(tableName).WithDescripton(docstring);
                    var newGlobals = code.Globals.AddOrUpdateDatabaseMembers(newTable);
                    return new ApplyCommandResult(newGlobals);
                }
            }
            else
            {
                return GetMissingSyntaxResult(code);
            }
        }

#endregion

        #region Columns

        private static ApplyCommandResult ApplyAlterColumnType(KustoCode code, CustomCommand commandRoot)
        {
            if (GetElementWithTag(commandRoot, "ColumnName") is { } columnNameElement
                && GetElementWithTag(commandRoot, "ColumnType") is { } columnTypeElement
                && GetColumnType(columnTypeElement) is ScalarSymbol columnType
                && TryGetDatabaseTableAndColumnNames(columnNameElement, out var databaseName, out var tableName, out var columnName))
            {
                var database = (databaseName == null)
                    ? code.Globals.Database 
                    : code.Globals.Cluster.GetDatabase(databaseName);
                if (database == null)
                    return GetEntityDoesNotExistResult(code, "database", databaseName!);
                var table = database.GetTable(tableName);
                if (table == null)
                    return GetEntityDoesNotExistResult(code, "table", tableName!);
                if (!table.TryGetColumn(columnName, out var column))
                    return GetEntityDoesNotExistResult(code, "column", columnName!);

                var newColumn = column.WithType(columnType);
                var newTable = table.AddOrUpdateColumns(newColumn);
                var newDatabase = database.AddOrUpdateMembers(newTable);
                var newGlobals = code.Globals.AddOrUpdateClusterDatabase(newDatabase);
                return new ApplyCommandResult(newGlobals);
            }

            return GetMissingSyntaxResult(code);
        }


        private static ApplyCommandResult ApplyDropColumn(KustoCode code, CustomCommand commandRoot)
        {
            if (GetElementWithTag(commandRoot, "ColumnName") is { } columnNameElement
                && TryGetDatabaseTableAndColumnNames(columnNameElement, out var databaseName, out var tableName, out var columnName))
            {
                var database = (databaseName == null)
                    ? code.Globals.Database
                    : code.Globals.Cluster.GetDatabase(databaseName);
                if (database == null)
                    return GetEntityDoesNotExistResult(code, "database", databaseName!);
                var table = database.GetTable(tableName);
                if (table == null)
                    return GetEntityDoesNotExistResult(code, "table", tableName!);
                if (!table.TryGetColumn(columnName, out var column))
                    return GetEntityDoesNotExistResult(code, "column", columnName!);

                var newTable = table.RemoveColumns(column!);
                var newDb = database.AddOrUpdateMembers(newTable);
                var newGlobals = code.Globals.AddOrUpdateClusterDatabase(newDb);
                return new ApplyCommandResult(newGlobals);
            }

            return GetMissingSyntaxResult(code);
        }

        private static ApplyCommandResult ApplyDropTableColumns(KustoCode code, CustomCommand commandRoot)
        {
            if (GetNameWithTag(commandRoot, "TableName") is { } tableName
                && GetNamesWithTag(commandRoot, "ColumnName") is { } columnNames)
            {
                if (code.Globals.Database.GetTable(tableName) is TableSymbol table)
                {
                    var columns = new List<ColumnSymbol>();
                    foreach (var columnName in columnNames)
                    {
                        if (table.TryGetColumn(columnName, out var column))
                        {
                            columns.Add(column);
                        }
                        else
                        {
                            return GetEntityDoesNotExistResult(code, "column", columnName);
                        }
                    }

                    var newTable = table.RemoveColumns(columns);
                    var newGlobals = code.Globals.AddOrUpdateDatabaseMembers(newTable);
                    return new ApplyCommandResult(newGlobals);
                }
                else
                {
                    return GetEntityDoesNotExistResult(code, "table", tableName);
                }
            }

            return GetMissingSyntaxResult(code);
        }

        private static ApplyCommandResult ApplyRenameColumn(KustoCode code, CustomCommand commandRoot)
        {
            if (GetElementWithTag(commandRoot, "ColumnName") is { } columnNameElement
                && GetNameWithTag(commandRoot, "NewColumnName") is string newColumnName
                && TryGetDatabaseTableAndColumnNames(columnNameElement, out var databaseName, out var tableName, out var columnName))
            {
                var database = (databaseName == null)
                    ? code.Globals.Database
                    : code.Globals.Cluster.GetDatabase(databaseName);
                if (database == null)
                    return GetEntityDoesNotExistResult(code, "database", databaseName!);
                var table = database.GetTable(tableName);
                if (table == null)
                    return GetEntityDoesNotExistResult(code, "table", tableName!);
                if (!table.TryGetColumn(columnName, out var column))
                    return GetEntityDoesNotExistResult(code, "column", columnName!);
                if (table.TryGetColumn(newColumnName, out _))
                    return GetEntityAlreadyExistsResult(code, "column", newColumnName);

                var newColumn = column.WithName(newColumnName);
                var newTable = table.ReplaceColumns((column, newColumn));
                var newDb = database.AddOrUpdateMembers(newTable);
                var newGlobals = code.Globals.AddOrUpdateClusterDatabase(newDb);
                return new ApplyCommandResult(newGlobals);
            }

            return GetMissingSyntaxResult(code);
        }

        private static ApplyCommandResult ApplyRenameColumns(KustoCode code, CustomCommand commandRoot)
        {
            if (GetElementsWithTag(commandRoot, "ColumnName") is { } columnNameElements)
            {
                var globals = code.Globals;

                foreach (var columnNameElement in columnNameElements)
                {
                    if (GetNameWithTag(columnNameElement.Parent, "NewColumnName") is string newColumnName
                        && TryGetDatabaseTableAndColumnNames(columnNameElement, out var databaseName, out var tableName, out var columnName))
                    {
                        var database = (databaseName == null)
                            ? globals.Database
                            : globals.Cluster.GetDatabase(databaseName);
                        if (database == null)
                            return GetEntityDoesNotExistResult(code, "database", databaseName!);
                        var table = database.GetTable(tableName);
                        if (table == null)
                            return GetEntityDoesNotExistResult(code, "table", tableName!);
                        if (!table.TryGetColumn(columnName, out var column))
                            return GetEntityDoesNotExistResult(code, "column", columnName!);
                        if (table.TryGetColumn(newColumnName, out _))
                            return GetEntityAlreadyExistsResult(code, "column", newColumnName);

                        var cluster = globals.GetCluster(database);

                        var newColumn = column.WithName(newColumnName);
                        var newTable = table.ReplaceColumns((column, newColumn));
                        var newDatabase = database.AddOrUpdateMembers(newTable);
                        globals = globals.AddOrUpdateClusterDatabase(newDatabase);
                    }
                    else
                    {
                        return GetMissingSyntaxResult(code, columnNameElement);
                    }
                }

                return new ApplyCommandResult(globals);
            }
            else
            {
                return GetMissingSyntaxResult(code);
            }
        }

        private static ApplyCommandResult ApplyAlterTableColumnDocStrings(KustoCode code, CustomCommand commandRoot, bool isMerge)
        {
            if (GetNameWithTag(commandRoot, "TableName") is string tableName
                && GetElementsWithTag(commandRoot, "ColumnName") is { } columnNameElements)
            {
                var updatedColumns = new Dictionary<string, ColumnSymbol>();

                if (code.Globals.Database.GetTable(tableName) is TableSymbol table)
                {
                    foreach (var columnNameElement in columnNameElements)
                    {
                        if (GetName(columnNameElement) is string columnName
                            && GetNameWithTag(columnNameElement.Parent, "DocString") is string docstring)
                        {
                            if (table.TryGetColumn(columnName, out var column))
                            {
                                var updatedColumn = column.WithDescription(docstring);
                                updatedColumns.Add(column.Name, updatedColumn);
                            }
                            else
                            {
                                return GetEntityDoesNotExistResult(code, "column", columnName);
                            }
                        }
                        else
                        {
                            return GetMissingSyntaxResult(code, columnNameElement);
                        }
                    }

                    if (!isMerge)
                    {
                        // clear description from any unspecified column
                        foreach (var column in table.Columns)
                        {
                            if (!updatedColumns.ContainsKey(column.Name)
                                && column.Description.Length > 0)
                            {
                                updatedColumns.Add(column.Name, column.WithDescription(""));
                            }
                        }
                    }

                    var newTable = table.AddOrUpdateColumns(updatedColumns.Values);
                    var newGlobals = code.Globals.AddOrUpdateDatabaseMembers(newTable);
                    return new ApplyCommandResult(newGlobals);
                }
                else
                {
                    return GetEntityDoesNotExistResult(code, "table", tableName);
                }
            }
            else
            {
                return GetMissingSyntaxResult(code);
            }
        }

        #endregion

        #region External Tables

        private static ApplyCommandResult ApplyCreateExternalTable(KustoCode code, CustomCommand commandRoot)
        {
            if (GetElementWithTag(commandRoot, "ExternalTableName") is { } nameElement
                && GetName(nameElement) is string externalTableName
                && GetSchemaAfterElement(code, nameElement) is string schema
                && GetPropertyValueText(commandRoot, "docstring") is var docstring)
            {
                if (code.Globals.Database.GetExternalTable(externalTableName) == null)
                {
                    var newExternalTable = new ExternalTableSymbol(externalTableName, schema, docstring);
                    var newGlobals = code.Globals.AddOrUpdateDatabaseMembers(newExternalTable);
                    return new ApplyCommandResult(newGlobals);
                }
                else
                {
                    return GetEntityAlreadyExistsResult(code, "external table", externalTableName);
                }
            }
            else
            {
                return GetMissingSyntaxResult(code);
            }
        }

        private static ApplyCommandResult ApplyAlterExternalTable(KustoCode code, CustomCommand commandRoot)
        {
            if (GetElementWithTag(commandRoot, "ExternalTableName") is { } nameElement
                && GetName(nameElement) is string externalTableName
                && GetSchemaAfterElement(code, nameElement) is string schema
                && GetPropertyValueText(commandRoot, "docstring") is var docstring)
            {
                if (code.Globals.Database.GetExternalTable(externalTableName) is ExternalTableSymbol externalTable)
                {
                    var newExternalTable = new ExternalTableSymbol(externalTableName, schema, docstring);
                    var newGlobals = code.Globals.AddOrUpdateDatabaseMembers(newExternalTable);
                    return new ApplyCommandResult(newGlobals);
                }
                else
                {
                    return GetEntityDoesNotExistResult(code, "external table", externalTableName);
                }
            }
            else
            {
                return GetMissingSyntaxResult(code);
            }
        }

        private static ApplyCommandResult ApplyCreateOrAlterExternalTable(KustoCode code, CustomCommand commandRoot)
        {
            if (GetElementWithTag(commandRoot, "ExternalTableName") is { } nameElement
                && GetName(nameElement) is string externalTableName
                && GetSchemaAfterElement(code, nameElement) is string schema
                && GetPropertyValueText(commandRoot, "docstring") is var docstring)
            {
                var newExternalTable = new ExternalTableSymbol(externalTableName, schema, docstring);
                var newGlobals = code.Globals.AddOrUpdateDatabaseMembers(newExternalTable);
                return new ApplyCommandResult(newGlobals);
            }
            else
            {
                return GetMissingSyntaxResult(code);
            }
        }

        private static ApplyCommandResult ApplyDropExternalTable(KustoCode code, CustomCommand commandRoot)
        {
            if (GetNameWithTag(commandRoot, "ExternalTableName") is string externalTableName)
            {
                if (code.Globals.Database.GetExternalTable(externalTableName) is ExternalTableSymbol externalTable)
                {
                    var newGlobals = code.Globals.RemoveDatabaseMembers(externalTable);
                    return new ApplyCommandResult(newGlobals);
                }
                else
                {
                    return GetEntityDoesNotExistResult(code, "external table", externalTableName);
                }
            }
            else
            {
                return GetMissingSyntaxResult(code);
            }
        }
        #endregion

        #region Materialized Views

        private static ApplyCommandResult ApplyCreateMaterializedView(KustoCode code, CustomCommand commandRoot)
        {
            if (GetNameWithTag(commandRoot, "MaterializedViewName") is string name
                && GetPropertyValueText(commandRoot, "docstring") is var docstring
                && commandRoot.GetFirstDescendant<FunctionBody>() is { } body)
            {
                if (!TryGetMaterializedViewSource(code, out var schema, out var diagnostics))
                    return new ApplyCommandResult(code.Globals, code.Text, new[] { Diagnostics.GetMaterializedViewSourceInvalid() }.Concat(diagnostics));

                if (code.Globals.Database.GetMaterializedView(name) == null)
                {
                    var newView = new MaterializedViewSymbol(name, schema.Columns, body.ToString(), docstring);
                    var newGlobals = code.Globals.AddOrUpdateDatabaseMembers(newView);
                    return new ApplyCommandResult(newGlobals);
                }
                else if (HasIfNotExists(commandRoot))
                {
                    return new ApplyCommandResult(code.Globals);
                }
                else
                {
                    return GetEntityAlreadyExistsResult(code, "materialized view", name);
                }
            }
            else
            {
                return GetMissingSyntaxResult(code);
            }
        }

        private static ApplyCommandResult ApplyAlterMaterializedView(KustoCode code, CustomCommand commandRoot)
        {
            if (GetNameWithTag(commandRoot, "MaterializedViewName") is string name
                && GetPropertyValueText(commandRoot, "docstring") is var docstring
                && commandRoot.GetFirstDescendant<FunctionBody>() is { } body)
            {
                if (!TryGetMaterializedViewSource(code, out var schema, out var diagnostics))
                    return new ApplyCommandResult(code.Globals, code.Text, new[] { Diagnostics.GetMaterializedViewSourceInvalid() }.Concat(diagnostics));

                if (code.Globals.Database.GetMaterializedView(name) is { } mview)
                {
                    var newView = new MaterializedViewSymbol(name, schema.Columns, body.ToString(), docstring);
                    var newGlobals = code.Globals.AddOrUpdateDatabaseMembers(newView);
                    return new ApplyCommandResult(newGlobals);
                }
                else
                {
                    return GetEntityDoesNotExistResult(code, "materialized view", name);
                }
            }
            else
            {
                return GetMissingSyntaxResult(code);
            }
        }

        private static ApplyCommandResult ApplyCreateOrAlterMaterializedView(KustoCode code, CustomCommand commandRoot)
        {
            if (GetNameWithTag(commandRoot, "MaterializedViewName") is string name
                && GetPropertyValueText(commandRoot, "docstring") is var docstring
                && commandRoot.GetFirstDescendant<FunctionBody>() is { } body)
            {
                if (!TryGetMaterializedViewSource(code, out var schema, out var diagnostics))
                    return new ApplyCommandResult(code.Globals, code.Text, new[] { Diagnostics.GetMaterializedViewSourceInvalid() }.Concat(diagnostics));

                var newView = new MaterializedViewSymbol(name, schema.Columns, body.ToString(), docstring);
                var newGlobals = code.Globals.AddOrUpdateDatabaseMembers(newView);
                return new ApplyCommandResult(newGlobals);
            }
            else
            {
                return GetMissingSyntaxResult(code);
            }
        }

        private static ApplyCommandResult ApplyAlterMaterializedViewDocString(KustoCode code, CustomCommand commandRoot)
        {
            if (GetNameWithTag(commandRoot, "MaterializedViewName") is string name
                && GetNameWithTag(commandRoot, "Documentation") is string docstring)
            {
                if (code.Globals.Database.GetMaterializedView(name) is { } existingView)
                {
                    var newView = existingView.WithDescripton(docstring);
                    var newGlobals = code.Globals.AddOrUpdateDatabaseMembers(newView);
                    return new ApplyCommandResult(newGlobals);
                }
                else
                {
                    return GetEntityDoesNotExistResult(code, "materialized view", name);
                }
            }
            else
            {
                return GetMissingSyntaxResult(code);
            }
        }

        private static ApplyCommandResult ApplyDropMaterializedView(KustoCode code, CustomCommand commandRoot)
        {
            if (GetElementAfterToken(commandRoot, "materialized-view") is { } nameElement
                && GetName(nameElement) is string name)
            {
                if (code.Globals.Database.GetMaterializedView(name) is MaterializedViewSymbol existingView)
                {
                    var newGlobals = code.Globals.RemoveDatabaseMembers(existingView);
                    return new ApplyCommandResult(newGlobals);
                }
                else
                {
                    return GetEntityDoesNotExistResult(code, "materialized view", name);
                }
            }
            else
            {
                return GetMissingSyntaxResult(code);
            }
        }

        private static ApplyCommandResult ApplyRenameMaterializedView(KustoCode code, CustomCommand commandRoot)
        {
            if (GetNameWithTag(commandRoot, "MaterializedViewName") is string name
                && GetNameWithTag(commandRoot, "NewMaterializedViewName") is string newName)
            {
                if (code.Globals.Database.GetMaterializedView(name) is MaterializedViewSymbol existingView)
                {
                    if (code.Globals.Database.GetMaterializedView(newName) == null)
                    {
                        var newView = existingView.WithName(newName);
                        var newGlobals = code.Globals.ReplaceDatabaseMembers((existingView, newView));
                        return new ApplyCommandResult(newGlobals);
                    }
                    else
                    {
                        return GetEntityAlreadyExistsResult(code, "materialized view", newName);
                    }
                }
                else
                {
                    return GetEntityDoesNotExistResult(code, "materialized view", name);
                }
            }
            else
            {
                return GetMissingSyntaxResult(code);
            }
        }

        #endregion

        #region Execute Script

        private static ApplyCommandResult ApplyExecuteDatabaseScript(KustoCode code, CustomCommand commandRoot)
        {
            var otherCommands = commandRoot.GetDescendants<Command>().Select(cc => cc.ToString());
            var globals = code.Globals;

            ApplyCommandResult? result = null;
            foreach (var otherCommand in otherCommands)
            {
                result = globals.ApplyCommand(otherCommand);
                if (!result.Succeeded)
                    return result;
                globals = result.Globals;
            }

            return result!;
        }

        #endregion

        #region Helpers

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
            GlobalState globals, 
            SyntaxElement expr,
            out DatabaseSymbol? database,
            out TableSymbol? table,
            out ColumnSymbol? column)
        {
            if (TryGetDatabaseTableAndColumnNames(expr, out var databaseName, out var tableName, out var columnName))
            {
                database = databaseName != null ? globals.Cluster.GetDatabase(databaseName) : globals.Database;
                table = database?.GetTable(tableName);
                column = table?.GetColumn(columnName);
                return database != null && table != null && column != null;
            }

            database = null;
            table = null;
            column = null;
            return false;
        }

        private static bool TryGetDatabaseTableAndColumnNames(
            SyntaxElement expr,
            out string? database,
            out string? table,
            out string? column)
        {
            if (expr is PathExpression pe)
            {
                if (pe.Expression is PathExpression dbPath
                    && dbPath.Expression is NameReference dbRef
                    && dbPath.Selector is NameReference dbTableRef
                    && pe.Selector is NameReference dbTableColumnRef)
                {
                    database = dbRef.SimpleName;
                    table = dbTableRef.SimpleName;
                    column = dbTableColumnRef.SimpleName;
                    return true;
                }
                else if (pe.Expression is NameReference tableRef
                    && pe.Selector is NameReference columnRef)
                {
                    database = null;
                    table = tableRef.SimpleName;
                    column = columnRef.SimpleName;
                    return true;
                }
            }
            else if (expr is CustomNode && expr.ChildCount == 1)
            {
                return TryGetDatabaseTableAndColumnNames(expr.GetChild(0), out database, out table, out column);
            }

            database = null;
            table = null;
            column = null;
            return false;
        }

        /// <summary>
        /// Gets the column type info from the element
        /// </summary>
        private static ScalarSymbol? GetColumnType(SyntaxElement element)
        {
            if (element is PrimitiveTypeExpression pex)
                return ScalarTypes.GetSymbol(pex.Type.Text);
            else if (element is CustomNode && element.ChildCount == 1)
                return GetColumnType(element.GetChild(0));
            return null;
        }

        /// <summary>
        /// Get's the schema immediately after the specified element.
        /// </summary>
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

        /// <summary>
        /// Gets the name from the element with the specified tag.
        /// </summary>
        private static string? GetNameWithTag(SyntaxElement root, string tag)
        {
            return GetName(GetElementWithTag(root, tag));
        }

        /// <summary>
        /// Gets the names from the elements with the specified tag.
        /// </summary>
        private static IReadOnlyList<string> GetNamesWithTag(SyntaxElement root, string tag)
        {
            return GetElementsWithTag(root, tag)
                .Where(e => e != null)
                .Select(n => GetName(n))
                .Where(n => n != null)
                .ToList()!;
        }

        /// <summary>
        /// Gets the elements with the specified tag.
        /// </summary>
        private static SyntaxElement? GetElementWithTag(SyntaxElement root, string tag)
        {
            return root.GetFirstDescendant<SyntaxElement>(e => e.NameInParent == tag);
        }

        /// <summary>
        /// Gets all the elements with the specified tag.
        /// </summary>
        private static IReadOnlyList<SyntaxElement> GetElementsWithTag(SyntaxElement root, string tag)
        {
            return root.GetDescendants<SyntaxElement>(e => e.NameInParent == tag);
        }

        /// <summary>
        /// Gets the name matching the selector.
        /// </summary>
        private static string? GetNameAt(SyntaxElement root, Func<SyntaxElement, bool> selector)
        {
            return GetName(GetElementAt(root, selector));
        }

        /// <summary>
        /// Gets the name immediately after the token with the specified text.
        /// </summary>
        private static string? GetNameAfterToken(SyntaxElement root, string tokenText)
        {
            var token = root.GetFirstDescendant<SyntaxToken>(tk => tk.Text == tokenText);
            return GetNameAfterElement(token);
        }

        /// <summary>
        /// Get's the name immediately after the given element.
        /// </summary>
        private static string? GetNameAfterElement(SyntaxElement element)
        {
            return GetName(GetNextPeer(element));
        }

        /// <summary>
        /// Get's the name represented by the element.
        /// </summary>
        private static string? GetName(SyntaxElement? element)
        {
            if (element is NameReference nr)
                return nr.SimpleName;
            else if (element is NameDeclaration nd)
                return nd.SimpleName;
            else if (element is LiteralExpression lit)
                return lit.LiteralValue.ToString();
            else if (element is SyntaxToken tok)
                return tok.ValueText;
            else if (element is CustomNode && element.ChildCount == 1)
                return GetName(element.GetChild(0));
            return null;

        }

        private static string? GetLiteralValueAfterToken(SyntaxElement root, string tokenText)
        {
            var token = root.GetFirstDescendant<SyntaxToken>(tk => tk.Text == tokenText);
            return GetLiteralValueAfterElement(token);
        }

        private static string? GetLiteralValueAfterElement(SyntaxElement? element)
        {
            return GetLiteralValue(GetNextPeer(element));
        }

        private static string? GetLiteralValueWithTag(SyntaxElement root, string tag)
        {
            return GetLiteralValue(GetElementWithTag(root, tag));
        }

        private static string? GetLiteralValue(SyntaxElement? element)
        {
            if (element is LiteralExpression lit)
                return lit.LiteralValue.ToString();
            return null;
        }

        private static SyntaxElement? GetElementAfterToken(SyntaxElement root, string tokenText)
        {
            var token = root.GetFirstDescendant<SyntaxToken>(tk => tk.Text == tokenText);
            return GetElementAfterElement(token);
        }

        private static SyntaxElement? GetElementAfterElement(SyntaxElement? element)
        {
            return GetNextPeer(element);
        }

        /// <summary>
        /// Gets the element matching the selector.
        /// </summary>
        private static SyntaxElement? GetElementAt(SyntaxElement root, Func<SyntaxElement, bool> selector)
        {
            return root.GetFirstDescendant<SyntaxElement>(e => selector(e));
        }

        /// <summary>
        /// Gets the elements matching the selector.
        /// </summary>
        private static IReadOnlyList<SyntaxElement> GetElementsAt(SyntaxElement root, Func<SyntaxElement, bool> selector)
        {
            return root.GetDescendants<SyntaxElement>(e => selector(e));
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

        private static bool TryGetMaterializedViewSource(KustoCode code, out TableSymbol schema, out IReadOnlyList<Diagnostic> diagnostics)
        {
            schema = null!;
            diagnostics = Array.Empty<Diagnostic>();

            var analyzedCode = code.Analyze();

            var commandRoot = analyzedCode.Syntax.GetFirstDescendant<CustomCommand>();
            if (commandRoot == null)
                return false;

            var body = commandRoot.GetFirstDescendant<FunctionBody>();
            if (body == null)
                return false;

            // any errors in body explicitly?
            var errors = GetErrors(body.GetContainedDiagnostics());
            if (errors.Count > 0)
            {
                diagnostics = errors;
                return false;
            }

            schema = (body.Expression?.ResultType as TableSymbol)!;
            return schema != null;
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

        private static SyntaxElement? GetNextPeer(SyntaxElement? element)
        {
            if (element == null)
                return null;

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
        #endregion
    }

    public sealed class ApplyCommandResult
    {
        /// <summary>
        /// The updated globals.
        /// </summary>
        public GlobalState Globals { get; }

        /// <summary>
        /// The command that caused the errors.
        /// </summary>
        public string Command { get; }

        /// <summary>
        /// The errors, if any.
        /// </summary>
        public IReadOnlyList<Diagnostic> Errors { get; }

        /// <summary>
        /// True if the command was applied successfully.
        /// </summary>
        public bool Succeeded => this.Errors.Count == 0;

        public ApplyCommandResult(GlobalState globals, string? commandWithError, IEnumerable<Diagnostic>? errors)
        {
            this.Globals = globals;
            this.Command = commandWithError ?? "";
            this.Errors = errors != null ? errors.ToReadOnly() : Array.Empty<Diagnostic>();
        }

        public ApplyCommandResult(GlobalState globals, string commandWithError, Diagnostic error)
            : this(globals, commandWithError, new[] { error })
        {
        }

        public ApplyCommandResult(GlobalState globals)
            : this(globals, null, (IEnumerable<Diagnostic>?)null)
        {
        }
    }
}
