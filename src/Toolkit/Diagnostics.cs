using Kusto.Language;

namespace Kusto.Toolkit
{
    internal static class Diagnostics
    {
        public static Diagnostic GetNoCurrentDatabase()
        {
            return new Diagnostic("KT001", "Cannot apply command. No current database or cluster defined.");
        }

        public static Diagnostic GetCommandTextIsNotCommandDiagnostic()
        {
            return new Diagnostic("KT002", "The command text does not refer to a command.");
        }

        public static Diagnostic GetCommandHasErrors()
        {
            return new Diagnostic("KT003", "The command has errors.");
        }

        public static Diagnostic GetCommandNotRecognized()
        {
            return new Diagnostic("KT004", "The command is incomplete or not recognized.");
        }

        public static Diagnostic GetCommandHasMissingSyntax()
        {
            return new Diagnostic("KT005", "The command has missing syntax needed to apply.");
        }

        public static Diagnostic GetEntityAlreadyExists(string kind, string name)
        {
            return new Diagnostic("KT006", $"Cannot create {kind} '{name}'. It already exists.");
        }

        public static Diagnostic GetEntityDoesNotExist(string kind, string name)
        {
            return new Diagnostic("KT007", $"The {kind} '{name}' does not exist");
        }

        public static Diagnostic GetMergeFailed()
        {
            return new Diagnostic("KT008", $"The schema merge failed due to conflicting column definitions.");
        }

        public static Diagnostic GetMaterializedViewSourceInvalid()
        {
            return new Diagnostic("KT009", $"The materialized-view source is invalid");
        }

        public static Diagnostic GetUnhandledCommandKind(string commandKind)
        {
            return new Diagnostic("KT010", $"The command '{commandKind}' cannot be applied.");
        }
    }
}
