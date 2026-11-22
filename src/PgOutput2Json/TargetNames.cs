using System;
using System.Collections.Generic;

namespace PgOutput2Json
{
    /// <summary>
    /// The name an output gives the table of a PostgreSQL table, from its GetTableName option (the schema and the
    /// table name apart, so a name with a dot needs no taking apart), made once per table. Two PostgreSQL tables
    /// with the same name in the output would mix their rows: the second one is an error. One instance per output
    /// factory, shared by its publishers (the main stream and the sync workers, which copy tables at the same time).
    /// </summary>
    internal sealed class TargetNames
    {
        private readonly Func<string, string, string> _getTableName;

        // by the joined name of the PostgreSQL table, and the other way round
        private readonly Dictionary<string, string> _targets = [];
        private readonly Dictionary<string, string> _sources = [];

        public TargetNames(Func<string, string, string> getTableName)
        {
            _getTableName = getTableName;
        }

        /// <summary>A table of the public schema keeps its name, any other is schema + separator + name.</summary>
        public static string Default(string schemaName, string relationName, string separator)
        {
            return schemaName == "public" ? relationName : string.Concat(schemaName, separator, relationName);
        }

        public string Get(string tableName, string schemaName, string relationName)
        {
            lock (_targets)
            {
                return GetLocked(tableName, schemaName, relationName);
            }
        }

        private string GetLocked(string tableName, string schemaName, string relationName)
        {
            if (_targets.TryGetValue(tableName, out var target)) return target;

            target = _getTableName(schemaName, relationName);

            if (string.IsNullOrEmpty(target)) throw new Exception($"GetTableName returned no name for the table {tableName}");

            if (_sources.TryGetValue(target, out var other))
            {
                throw new Exception($"The tables {other} and {tableName} both have the name {target} in the output, and their rows would mix. " +
                    "Give one of them another name with GetTableName.");
            }

            _sources[target] = tableName;
            _targets[tableName] = target;

            return target;
        }
    }
}
