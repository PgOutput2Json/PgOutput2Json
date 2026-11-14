using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;

namespace PgOutput2Json.ClickHouse
{
    internal enum ColumnKind
    {
        Other,
        Date,
        Timestamp,
    }

    internal sealed class ClickHouseColumn
    {
        public required string Name { get; init; }
        public required string QuotedName { get; init; }
        public required bool IsKey { get; init; }

        /// <summary>
        /// The ClickHouse type without Nullable.
        /// </summary>
        public required string Type { get; init; }

        public required ColumnKind Kind { get; init; }
    }

    /// <summary>
    /// The ClickHouse table of a PostgreSQL table, with the rows that wait for the next insert.
    /// </summary>
    internal sealed class ClickHouseTable
    {
        public const string VersionColumn = "_version";
        public const string DeletedColumn = "_deleted";

        public string QuotedName { get; }

        public IReadOnlyList<ClickHouseColumn> Columns { get; }
        public IReadOnlyList<ClickHouseColumn> KeyColumns { get; }

        /// <summary>
        /// True when the key columns can be null: every column is a key column, as with REPLICA IDENTITY FULL.
        /// </summary>
        public bool HasNullableKey { get; }

        public string UpsertQuery { get; }
        public string DeleteQuery { get; }

        public StringBuilder Upserts { get; } = new();
        public StringBuilder Deletes { get; } = new();

        public ClickHouseTable(string tableName, IReadOnlyList<ClickHouseColumn> columns)
        {
            QuotedName = Quote(tableName);

            Columns = columns;
            KeyColumns = columns.Where(c => c.IsKey).ToList();

            HasNullableKey = KeyColumns.Count == columns.Count;

            UpsertQuery = $"INSERT INTO {QuotedName} ({string.Join(", ", columns.Select(c => c.QuotedName))}, {VersionColumn}, {DeletedColumn}) FORMAT JSONCompactEachRow";
            DeleteQuery = $"INSERT INTO {QuotedName} ({string.Join(", ", KeyColumns.Select(c => c.QuotedName))}, {VersionColumn}, {DeletedColumn}) FORMAT JSONCompactEachRow";
        }

        /// <summary>
        /// The type of the column in the table. Only the columns of a real key are not Nullable.
        /// </summary>
        public string GetColumnType(ClickHouseColumn column)
        {
            return column.IsKey && !HasNullableKey ? column.Type : $"Nullable({column.Type})";
        }

        public string GetCreateStatement()
        {
            var sql = new StringBuilder($"CREATE TABLE IF NOT EXISTS {QuotedName} (");

            foreach (var column in Columns)
            {
                sql.Append(column.QuotedName).Append(' ').Append(GetColumnType(column)).Append(", ");
            }

            sql.Append($"{VersionColumn} UInt128, {DeletedColumn} UInt8) ");

            if (KeyColumns.Count == 0)
            {
                // without a key PostgreSQL publishes only inserts, and there is nothing to replace a row by
                sql.Append("ENGINE = MergeTree ORDER BY tuple()");
            }
            else
            {
                sql.Append($"ENGINE = ReplacingMergeTree({VersionColumn}, {DeletedColumn}) ORDER BY ({string.Join(", ", KeyColumns.Select(c => c.QuotedName))})");

                if (HasNullableKey) sql.Append(" SETTINGS allow_nullable_key = 1");
            }

            return sql.ToString();
        }

        /// <summary>
        /// Adds the columns the table does not have yet. A column that PostgreSQL no longer has is kept.
        /// </summary>
        public string GetAlterStatement()
        {
            return $"ALTER TABLE {QuotedName} " + string.Join(", ", Columns
                .Where(c => !c.IsKey)
                .Select(c => $"ADD COLUMN IF NOT EXISTS {c.QuotedName} {GetColumnType(c)}"));
        }

        public static string Quote(string identifier)
        {
            return $"`{identifier.Replace("\\", "\\\\").Replace("`", "\\`")}`";
        }

        /// <summary>
        /// Reads the schema of the compact write mode: the table name, then [name, isKey, type oid, type modifier] per column.
        /// </summary>
        public static ClickHouseTable FromSchema(string tableName, JsonElement schemaElement)
        {
            if (schemaElement.ValueKind != JsonValueKind.Array) throw new Exception("Invalid schema - expected array");

            var columns = new List<ClickHouseColumn>();

            var length = schemaElement.GetArrayLength();

            for (var i = 1; i < length; i++)
            {
                var columnElement = schemaElement[i];

                if (columnElement.ValueKind != JsonValueKind.Array || columnElement.GetArrayLength() < 3)
                {
                    throw new Exception("Invalid schema - expected name, key flag and type for column " + i);
                }

                var name = columnElement[0].GetString() ?? string.Empty;
                var oid = columnElement[2].GetUInt32();

                var typeModifier = columnElement.GetArrayLength() > 3 && columnElement[3].TryGetInt32(out var modifier) ? modifier : -1;

                columns.Add(new ClickHouseColumn
                {
                    Name = name,
                    QuotedName = Quote(name),
                    IsKey = columnElement[1].GetInt32() == 1,
                    Type = GetType(oid, typeModifier),
                    Kind = oid == (uint)PgOid.DATEOID ? ColumnKind.Date
                        : oid is (uint)PgOid.TIMESTAMPOID or (uint)PgOid.TIMESTAMPTZOID ? ColumnKind.Timestamp
                        : ColumnKind.Other,
                });
            }

            return new ClickHouseTable(tableName, columns);
        }

        /// <summary>
        /// Anything without a type of its own in ClickHouse is a String with the text PostgreSQL sends.
        /// Arrays and json are Strings too, with the JSON text.
        /// </summary>
        internal static string GetType(uint oid, int typeModifier)
        {
            switch ((PgOid)oid)
            {
                case PgOid.BOOLOID: return "Bool";
                case PgOid.INT2OID: return "Int16";
                case PgOid.INT4OID: return "Int32";
                case PgOid.INT8OID: return "Int64";
                case PgOid.OIDOID: return "UInt32";
                case PgOid.FLOAT4OID: return "Float32";
                case PgOid.FLOAT8OID: return "Float64";
                case PgOid.UUIDOID: return "UUID";
                case PgOid.DATEOID: return "Date32";
                case PgOid.TIMESTAMPOID: return "DateTime64(6)";
                case PgOid.TIMESTAMPTZOID: return "DateTime64(6, 'UTC')";
                case PgOid.NUMERICOID: return GetNumericType(typeModifier);
                default: return "String";
            }
        }

        private static string GetNumericType(int typeModifier)
        {
            // a numeric without precision can hold any number - this is the widest decimal ClickHouse has
            if (typeModifier < 4) return "Decimal(76, 38)";

            var precision = ((typeModifier - 4) >> 16) & 0xffff;
            var scale = (((typeModifier - 4) & 0x7ff) ^ 1024) - 1024;

            // a negative scale, a scale above the precision or too many digits have no decimal type in ClickHouse
            return scale >= 0 && scale <= precision && precision <= 76 ? $"Decimal({precision}, {scale})" : "String";
        }
    }
}
