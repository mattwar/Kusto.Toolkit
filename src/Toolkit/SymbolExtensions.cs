using System;
using System.Collections.Generic;
using System.Linq;
using Kusto.Language;
using Kusto.Language.Symbols;

namespace Tests
{
    public static class SymbolExtensions
    {
        /// <summary>
        /// Adds or updates members of the <see cref="DatabaseSymbol"/>.
        /// </summary>
        public static ClusterSymbol AddOrUpdateDatabases(this ClusterSymbol cluster, IEnumerable<DatabaseSymbol> newDatabases)
        {
            var databaseList = cluster.Databases.ToList();
            var changed = false;

            foreach (var newDb in newDatabases)
            {
                var existingDb = cluster.GetDatabase(newDb.Name);
                if (existingDb == null)
                {
                    databaseList.Add(newDb);
                    changed = true;
                }
                else
                {
                    var index = databaseList.IndexOf(existingDb);
                    if (index >= 0)
                    {
                        databaseList[index] = newDb;
                        changed = true;
                    }
                }
            }

            return changed ? cluster.WithDatabases(databaseList) : cluster;
        }

        /// <summary>
        /// Adds or updates members of the <see cref="DatabaseSymbol"/>.
        /// </summary>
        public static ClusterSymbol AddOrUpdateDatabases(this ClusterSymbol cluster, params DatabaseSymbol[] newDatabases)
        {
            return cluster.AddOrUpdateDatabases((IReadOnlyList<DatabaseSymbol>)newDatabases);
        }

        /// <summary>
        /// Adds or updates members of the <see cref="DatabaseSymbol"/>
        /// </summary>
        public static DatabaseSymbol AddOrUpdateMembers(this DatabaseSymbol database, params Symbol[] newMembers)
        {
            return AddOrUpdateMembers(database, (IEnumerable<Symbol>)newMembers);
        }

        /// <summary>
        /// Adds or updates members of the <see cref="DatabaseSymbol"/>.
        /// </summary>
        public static DatabaseSymbol AddOrUpdateMembers(this DatabaseSymbol database, IEnumerable<Symbol> newMembers)
        {
            if (newMembers.Count() == 1
                && database.GetMember(newMembers.ElementAt(0).Name) == null)
            {
                return database.AddMembers(newMembers);
            }

            var memberList = database.Members.ToList();
            var changed = false;

            foreach (var newMember in newMembers)
            {
                var existingMember = database.GetMember(newMember.Name);
                if (existingMember == null)
                {
                    memberList.Add(newMember);
                    changed = true;
                }
                else
                {
                    var index = memberList.IndexOf(existingMember);
                    if (index >= 0)
                    {
                        memberList[index] = newMember;
                        changed = true;
                    }
                }
            }

            return changed ? database.WithMembers(memberList) : database;
        }

        /// <summary>
        /// Removes members from the database symbol
        /// </summary>
        public static DatabaseSymbol RemoveMembers(this DatabaseSymbol database, IEnumerable<Symbol> members)
        {
            var newMemberList = database.Members.Except(members).ToList();
            return database.WithMembers(newMemberList);
        }

        /// <summary>
        /// Removes members from the database symbol
        /// </summary>
        public static DatabaseSymbol RemoveMembers(this DatabaseSymbol database, params Symbol[] members)
        {
            return RemoveMembers(database, (IEnumerable<Symbol>)members);
        }

        /// <summary>
        /// Replaces one or more existing members with alternate members.
        /// </summary>
        public static DatabaseSymbol ReplaceMembers<TMember>(this DatabaseSymbol database, IEnumerable<(TMember original, TMember replacement)> replacements)
            where TMember : Symbol
        {
            var list = database.Members.ToList();
            var changed = false;

            foreach (var pair in replacements)
            {
                var index = list.IndexOf(pair.original);
                if (index >= 0)
                {
                    list[index] = pair.replacement;
                    changed = true;
                }
            }

            return changed
                ? database.WithMembers(list)
                : database;
        }

        /// <summary>
        /// Replaces one or more existing database members with alternate members.
        /// </summary>
        public static DatabaseSymbol ReplaceMembers<TMember>(this DatabaseSymbol database, params (TMember original, TMember replacement)[] replacements)
            where TMember : Symbol
        {
            return database.ReplaceMembers((IEnumerable<(TMember original, TMember replacement)>)replacements);
        }

        /// <summary>
        /// Adds or updates columns of the <see cref="TableSymbol"/>.
        /// </summary>
        public static TableSymbol AddOrUpdateColumns(this TableSymbol table, IEnumerable<ColumnSymbol> newColumns)
        {
            var columnList = table.Columns.ToList();
            var changed = false;

            foreach (var newColumn in newColumns)
            {
                var existingColumn = columnList.FirstOrDefault(c => c.Name == newColumn.Name);
                if (existingColumn == null)
                {
                    columnList.Add(newColumn);
                    changed = true;
                }
                else
                {
                    var index = columnList.IndexOf(existingColumn);
                    if (index >= 0)
                    {
                        columnList[index] = newColumn;
                        changed = true;
                    }
                }
            }

            return changed
                ? table.WithColumns(columnList)
                : table;
        }

        /// <summary>
        /// Adds or updates columns of the <see cref="TableSymbol"/>.
        /// </summary>
        public static TableSymbol AddOrUpdateColumns(this TableSymbol table, params ColumnSymbol[] newColumns)
        {
            return table.AddOrUpdateColumns((IEnumerable<ColumnSymbol>)newColumns);
        }

        /// <summary>
        /// Removes the specified columns from the table.
        /// </summary>
        public static TableSymbol RemoveColumns(this TableSymbol table, IEnumerable<ColumnSymbol> columns)
        {
            return table.WithColumns(table.Columns.Except(columns));
        }

        /// <summary>
        /// Removes the specified columns from the table.
        /// </summary>
        public static TableSymbol RemoveColumns(this TableSymbol table, params ColumnSymbol[] columns)
        {
            return table.RemoveColumns((IEnumerable<ColumnSymbol>)columns);
        }

        /// <summary>
        /// Replaces one or more existing columns with alternate columns.
        /// </summary>
        public static TableSymbol ReplaceColumns(this TableSymbol table, IEnumerable<(ColumnSymbol original, ColumnSymbol replacement)> replacements)
        {
            var list = table.Columns.ToList();
            var changed = false;

            foreach (var pair in replacements)
            {
                var index = list.IndexOf(pair.original);
                if (index >= 0)
                {
                    list[index] = pair.replacement;
                    changed = true;
                }
            }

            return changed
                ? table.WithColumns(list)
                : table;
        }

        /// <summary>
        /// Replaces one or more existing columns with alternate columns.
        /// </summary>
        public static TableSymbol ReplaceColumns(this TableSymbol table, params (ColumnSymbol original, ColumnSymbol replacement)[] replacements)
        {
            return table.ReplaceColumns((IEnumerable<(ColumnSymbol original, ColumnSymbol replacement)>)replacements);
        }
    }
}
