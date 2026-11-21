using System.Text;

namespace PgOutput2Json
{
    /// <summary>
    /// The joined name of a table, schema.table, as the library uses it (the "t" of the JSON, the table name of a message,
    /// the keys of the options and of the table states). A schema or a table name with a dot in it is quoted, with its
    /// double quotes doubled ("x.y".table, schema."ab""c.d"), so the joined name is not ambiguous; any other is as it is.
    /// </summary>
    internal static class PgName
    {
        public static void Append(StringBuilder builder, string schemaName, string relationName)
        {
            if (!NeedsQuotes(schemaName) && !NeedsQuotes(relationName))
            {
                builder.Append(schemaName).Append('.').Append(relationName);
                return;
            }

            AppendPart(builder, schemaName);
            builder.Append('.');
            AppendPart(builder, relationName);
        }

        private static bool NeedsQuotes(string name) => name.Contains('.');

        private static void AppendPart(StringBuilder builder, string name)
        {
            if (!NeedsQuotes(name))
            {
                builder.Append(name);
                return;
            }

            builder.Append('"');

            foreach (var c in name)
            {
                if (c == '"') builder.Append('"');
                builder.Append(c);
            }

            builder.Append('"');
        }
    }
}
