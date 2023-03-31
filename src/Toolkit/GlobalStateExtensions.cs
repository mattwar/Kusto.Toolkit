using System;
using System.Collections.Generic;
using System.Linq;
using Kusto.Language;
using Kusto.Language.Symbols;

namespace Tests
{
    public static partial class GlobalStateExtensions
    {
        /// <summary>
        /// Adds or updates members of the default database.
        /// </summary>
        public static GlobalState AddOrUpdateDatabaseMembers(this GlobalState globals, IEnumerable<Symbol> newMembers)
        {
            return globals.AddOrUpdateClusterDatabase(globals.Database.AddOrUpdateMembers(newMembers));
        }

        /// <summary>
        /// Adds or updates members of the default database.
        /// </summary>
        public static GlobalState AddOrUpdateDatabaseMembers(this GlobalState globals, params Symbol[] newMembers)
        {
            return globals.AddOrUpdateDatabaseMembers((IEnumerable<Symbol>)newMembers);
        }

        /// <summary>
        /// Removes members from the default database.
        /// </summary>
        public static GlobalState RemoveDatabaseMembers(this GlobalState globals, IEnumerable<Symbol> members)
        {
            return globals.AddOrUpdateClusterDatabase(globals.Database.RemoveMembers(members));
        }

        /// <summary>
        /// Removes members from the default database.
        /// </summary>
        public static GlobalState RemoveDatabaseMembers(this GlobalState globals, params Symbol[] members)
        {
            return globals.RemoveDatabaseMembers((IEnumerable<Symbol>)members);
        }

        /// <summary>
        /// Replace one or more original members of the default database with alternate members.
        /// </summary>
        public static GlobalState ReplaceDatabaseMembers<TMember>(this GlobalState globals, IReadOnlyList<(TMember original, TMember replacement)> replacements)
            where TMember : Symbol
        {
            return globals.AddOrUpdateClusterDatabase(globals.Database.ReplaceMembers(replacements));
        }

        /// <summary>
        /// Replace one or more original members of the default database with alternate members.
        /// </summary>
        public static GlobalState ReplaceDatabaseMembers<TMember>(this GlobalState globals, params (TMember original, TMember replacement)[] replacements)
            where TMember : Symbol
        {
            return globals.ReplaceDatabaseMembers((IReadOnlyList<(TMember original, TMember replacement)>)replacements);
        }

        /// <summary>
        /// Adds or updates the database within the default cluster.
        /// </summary>
        public static GlobalState AddOrUpdateClusterDatabase(this GlobalState globals, DatabaseSymbol newDatabase)
        {
            return globals.WithCluster(globals.Cluster.AddOrUpdateDatabase(newDatabase));
        }

        /// <summary>
        /// Adds or updates the databases within the default cluster.
        /// </summary>
        public static GlobalState AddOrUpdateClusterDatabases(this GlobalState globals, IReadOnlyList<DatabaseSymbol> newDatabases)
        {
            return globals.WithCluster(globals.Cluster.AddOrUpdateDatabases(newDatabases));
        }

        /// <summary>
        /// Adds or updates the databases within the default cluster.
        /// </summary>
        public static GlobalState AddOrUpdateClusterDatabases(this GlobalState globals, params DatabaseSymbol[] newDatabases)
        {
            return globals.AddOrUpdateClusterDatabases((IReadOnlyList<DatabaseSymbol>)newDatabases);
        }

    }
}
