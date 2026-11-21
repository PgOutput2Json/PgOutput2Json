using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;

namespace PgOutput2Json.ClickHouse
{
    internal enum ColumnKind
    {
        Other,
        Date,
        Timestamp,
        Json,
        Array,
        DateArray,
        TimestampArray,

        /// <summary>
        /// An array of a type that is not built in (an enum, a user type): the value is the PostgreSQL text of the array.
        /// </summary>
        TextArray,
    }

    internal sealed class ClickHouseColumn
    {
        public required string Name { get; init; }
        public required string QuotedName { get; init; }
        public required bool IsKey { get; init; }

        /// <summary>The PostgreSQL type, for the values of the rows that come as text.</summary>
        public uint TypeId { get; init; }

        /// <summary>
        /// The ClickHouse type without Nullable.
        /// </summary>
        public required string Type { get; init; }

        public required ColumnKind Kind { get; set; }

        /// <summary>
        /// A jsonb column: PostgreSQL keeps one value per key, so its objects have no key twice.
        /// </summary>
        public bool IsJsonb { get; init; }

        /// <summary>
        /// For a timestamp column (or an array of them) that is a DateTime64 with its own time zone in ClickHouse, which the
        /// output creates: that type (of the elements of an array). Its values are sent as the number ClickHouse stores,
        /// converted by the output (PostgreSQL's rules for a time of a time zone), also in JSON. Null: the text is sent,
        /// and ClickHouse converts it (a DateTime64 without a time zone, or another type).
        /// </summary>
        public ClickHouseBinaryType? DateTime64Type { get; set; }

        public bool IsArray => Kind is ColumnKind.Array or ColumnKind.DateArray or ColumnKind.TimestampArray or ColumnKind.TextArray;
    }

    /// <summary>
    /// The ClickHouse table of a PostgreSQL table, with the rows that wait for the next insert.
    /// </summary>
    internal sealed class ClickHouseTable
    {
        public const string VersionColumn = "_version";
        public const string DeletedColumn = "_deleted";

        /// <summary>
        /// When ClickHouse wrote the row, filled in by ClickHouse. It is not in the inserts, so a replayed change gets a new time.
        /// </summary>
        public const string SyncedAtColumn = "_synced_at";

        public string Name { get; }
        public string QuotedName { get; }

        /// <summary>The <see cref="RowTable"/> the table was made from, for the rows of an <see cref="IRowPublisher"/>.</summary>
        public RowTable? Source { get; set; }

        public IReadOnlyList<ClickHouseColumn> Columns { get; }
        public IReadOnlyList<ClickHouseColumn> KeyColumns { get; }

        /// <summary>
        /// True when the key columns can be null: every column is a key column, as with REPLICA IDENTITY FULL.
        /// </summary>
        public bool HasNullableKey { get; }

        public ClickHouseTableEngine Engine { get; }

        /// <summary>
        /// The columns of the ORDER BY: the key columns, in the order of the options or of the table.
        /// </summary>
        public IReadOnlyList<ClickHouseColumn> OrderByColumns { get; }

        public string? PartitionBy { get; }

        /// <summary>
        /// Set once the warning that a large value cannot be read from a Null table was logged.
        /// </summary>
        public bool UnchangedValueWarned { get; set; }

        public string UpsertQuery { get; private set; }
        public string DeleteQuery { get; private set; }

        public StringBuilder Upserts { get; } = new();
        public StringBuilder Deletes { get; } = new();

        /// <summary>The rows waiting in Upserts or BinaryUpserts, and in Deletes or BinaryDeletes.</summary>
        public int UpsertCount { get; set; }
        public int DeleteCount { get; set; }

        /// <summary>
        /// The RowBinary of each column, by the type the column has in ClickHouse, or null if the rows are sent as JSON:
        /// ClickHouse does not convert the values of RowBinary, and a type the output does not create could be any.
        /// </summary>
        public ClickHouseBinaryType[]? BinaryTypes { get; private set; }

        /// <summary>The RowBinary of the key columns, in their order, for the deletes.</summary>
        public ClickHouseBinaryType[]? BinaryKeyTypes { get; private set; }

        public RowBinaryWriter? BinaryUpserts { get; private set; }
        public RowBinaryWriter? BinaryDeletes { get; private set; }

        public string BinaryUpsertQuery { get; }
        public string BinaryDeleteQuery { get; }

        public ClickHouseTable(string tableName, IReadOnlyList<ClickHouseColumn> columns, ClickHouseTableOptions? options = null)
        {
            Name = tableName;
            QuotedName = Quote(tableName);

            Columns = columns;
            KeyColumns = columns.Where(c => c.IsKey).ToList();

            HasNullableKey = KeyColumns.Count == columns.Count;

            Engine = options?.Engine ?? ClickHouseTableEngine.ReplacingMergeTree;
            PartitionBy = string.IsNullOrWhiteSpace(options?.PartitionBy) ? null : options!.PartitionBy;
            OrderByColumns = GetOrderByColumns(tableName, options?.OrderBy);

            if (Engine == ClickHouseTableEngine.Null && (options?.OrderBy != null || PartitionBy != null))
            {
                throw new Exception($"Table {tableName}: a table with the Null engine has no ORDER BY and no PARTITION BY");
            }

            if (PartitionBy != null && KeyColumns.Count > 0)
            {
                var others = Identifiers(PartitionBy).Intersect(columns.Where(c => !c.IsKey).Select(c => c.Name)).ToList();

                if (others.Count > 0)
                {
                    throw new Exception($"Table {tableName}: the PARTITION BY uses {string.Join(", ", others)}, which is not a key column. " +
                        "ClickHouse replaces rows only within a partition, and the row of a delete has only the key columns. " +
                        "To partition by it, make it part of the replica identity (REPLICA IDENTITY USING INDEX on a unique index).");
                }
            }

            UpsertQuery = $"INSERT INTO {QuotedName} ({string.Join(", ", columns.Select(c => c.QuotedName))}, {VersionColumn}, {DeletedColumn}) FORMAT JSONCompactEachRow";
            DeleteQuery = $"INSERT INTO {QuotedName} ({string.Join(", ", KeyColumns.Select(c => c.QuotedName))}, {VersionColumn}, {DeletedColumn}) FORMAT JSONCompactEachRow";

            BinaryUpsertQuery = UpsertQuery.Replace("FORMAT JSONCompactEachRow", "FORMAT RowBinary");
            BinaryDeleteQuery = DeleteQuery.Replace("FORMAT JSONCompactEachRow", "FORMAT RowBinary");
        }

        /// <summary>
        /// An array column that is not an array in ClickHouse (created before the array types, or changed to a String
        /// after a multi-dimensional value) gets the JSON text, which ClickHouse turns into a string.
        /// </summary>
        /// <summary>
        /// Sends the rows in RowBinary if every column has a type it covers, and returns null, or else the column
        /// that keeps the rows in JSON.
        /// </summary>
        public string? UseRowBinary(IReadOnlyDictionary<string, string> types, TimeZoneInfo? serverTimeZone)
        {
            var binaryTypes = new ClickHouseBinaryType[Columns.Count];

            for (var i = 0; i < Columns.Count; i++)
            {
                if (!types.TryGetValue(Columns[i].Name, out var type)) return $"{Columns[i].Name} (not found)";

                // a DateTime64 without a time zone needs the one of the server, if it is not known
                var binaryType = ClickHouseBinaryType.Parse(type, serverTimeZone);

                if (binaryType == null) return $"{Columns[i].Name} {type}";

                binaryTypes[i] = binaryType;
            }

            // the version and the deleted flag are written as the types the output creates
            if (!types.TryGetValue(VersionColumn, out var version) || version != "UInt128") return $"{VersionColumn} {version}";
            if (!types.TryGetValue(DeletedColumn, out var deleted) || deleted != "UInt8") return $"{DeletedColumn} {deleted}";

            BinaryTypes = binaryTypes;
            BinaryKeyTypes = KeyColumns.Select(k => binaryTypes[Columns.ToList().IndexOf(k)]).ToArray();
            BinaryUpserts ??= new RowBinaryWriter();
            BinaryDeletes ??= new RowBinaryWriter();

            return null;
        }

        /// <summary>
        /// The JSON inserts read a number for a DateTime64 as the raw value, as servers before 26.8 did: the timestamps the
        /// output converts are sent as numbers in units of the precision of the column, not as seconds.
        /// </summary>
        public void ReadDateTimeNumbersAsRawValues()
        {
            const string format = " FORMAT JSONCompactEachRow";
            const string settings = " SETTINGS input_format_read_datetime_number_as_raw_value = 1" + format;

            if (!UpsertQuery.EndsWith(settings, StringComparison.Ordinal)) UpsertQuery = UpsertQuery.Replace(format, settings);
            if (!DeleteQuery.EndsWith(settings, StringComparison.Ordinal)) DeleteQuery = DeleteQuery.Replace(format, settings);
        }

        public void UseExistingTypes(IReadOnlyDictionary<string, string> types)
        {
            foreach (var column in Columns)
            {
                if (column.IsArray
                    && types.TryGetValue(column.Name, out var type)
                    && !type.StartsWith("Array(", StringComparison.Ordinal))
                {
                    column.Kind = ColumnKind.Other;
                }

                // without the time zone of the server, a DateTime64 without its own is not parsed
                var binaryType = column.Kind is ColumnKind.Timestamp or ColumnKind.TimestampArray && types.TryGetValue(column.Name, out var columnType)
                    ? ClickHouseBinaryType.Parse(columnType, serverTimeZone: null)
                    : null;

                var element = binaryType?.Element ?? binaryType;

                column.DateTime64Type = element?.Base == BinaryBase.DateTime64 && (binaryType!.Element != null) == (column.Kind == ColumnKind.TimestampArray)
                    ? element
                    : null;
            }
        }

        /// <summary>
        /// The type of the column in the table. Only the columns of a real key are not Nullable, and the arrays,
        /// which ClickHouse does not allow to be Nullable: a null array is an empty one.
        /// </summary>
        public string GetColumnType(ClickHouseColumn column)
        {
            if (column.IsArray) return column.Type;

            return column.IsKey && !HasNullableKey ? column.Type : $"Nullable({column.Type})";
        }

        private IReadOnlyList<ClickHouseColumn> GetOrderByColumns(string tableName, IReadOnlyList<string>? orderBy)
        {
            if (orderBy == null) return KeyColumns;

            var result = new List<ClickHouseColumn>();

            foreach (var name in orderBy)
            {
                var column = Columns.FirstOrDefault(c => c.Name == name)
                    ?? throw new Exception($"Table {tableName}: the ORDER BY column {name} is not a column of the table");

                if (result.Contains(column)) throw new Exception($"Table {tableName}: the ORDER BY has {name} twice");

                result.Add(column);
            }

            // ClickHouse replaces and deletes the rows by the ORDER BY: every key column, and no other
            if (KeyColumns.Count > 0 && (result.Count != KeyColumns.Count || result.Any(c => !c.IsKey)))
            {
                throw new Exception($"Table {tableName}: the ORDER BY must have exactly the key columns ({string.Join(", ", KeyColumns.Select(c => c.Name))}), " +
                    "because ClickHouse replaces and deletes the rows by it. To sort by another column, make it part of the replica identity " +
                    "(REPLICA IDENTITY USING INDEX on a unique index).");
            }

            return result;
        }

        /// <summary>
        /// The names in an expression: plain and quoted identifiers, but not what is in string literals.
        /// </summary>
        internal static IEnumerable<string> Identifiers(string expression)
        {
            foreach (Match match in Regex.Matches(expression, @"'(?:[^'\\]|\\.)*'|`((?:[^`\\]|\\.)*)`|""((?:[^""\\]|\\.)*)""|([A-Za-z_][A-Za-z0-9_]*)"))
            {
                for (var group = 1; group <= 3; group++)
                {
                    if (match.Groups[group].Success) yield return match.Groups[group].Value;
                }
            }
        }

        public string GetCreateStatement(TimeSpan? cleanUpDeletedRowsAfter)
        {
            var sql = new StringBuilder($"CREATE TABLE IF NOT EXISTS {QuotedName} (");

            foreach (var column in Columns)
            {
                sql.Append(column.QuotedName).Append(' ').Append(GetColumnType(column)).Append(", ");
            }

            sql.Append($"{VersionColumn} UInt128, {DeletedColumn} UInt8, {SyncedAtColumn} DateTime64(6, 'UTC') DEFAULT now64(6)) ");

            if (Engine == ClickHouseTableEngine.Null)
            {
                return sql.Append("ENGINE = Null").ToString();
            }

            // without a key PostgreSQL publishes only inserts, and there is nothing to replace a row by
            var replacing = Engine == ClickHouseTableEngine.ReplacingMergeTree && KeyColumns.Count > 0;

            sql.Append(replacing ? $"ENGINE = ReplacingMergeTree({VersionColumn}, {DeletedColumn})" : "ENGINE = MergeTree");

            if (PartitionBy != null) sql.Append(" PARTITION BY ").Append(PartitionBy);

            sql.Append(OrderByColumns.Count == 0 ? " ORDER BY tuple()" : $" ORDER BY ({string.Join(", ", OrderByColumns.Select(c => c.QuotedName))})");

            var settings = new List<string>();

            // the columns of a table without a key, and all of them with REPLICA IDENTITY FULL, are Nullable
            if (HasNullableKey || (KeyColumns.Count == 0 && (OrderByColumns.Count > 0 || PartitionBy != null))) settings.Add("allow_nullable_key = 1");

            if (replacing && cleanUpDeletedRowsAfter is { } after) settings.Add(GetCleanupSettings(after));

            if (settings.Count > 0) sql.Append(" SETTINGS ").Append(string.Join(", ", settings));

            return sql.ToString();
        }

        /// <summary>
        /// The cleanup settings for a table that existed before: null for a table without a key, which has no deleted rows.
        /// </summary>
        public string? GetCleanupStatement(TimeSpan? cleanUpDeletedRowsAfter)
        {
            if (cleanUpDeletedRowsAfter is not { } after || KeyColumns.Count == 0 || Engine != ClickHouseTableEngine.ReplacingMergeTree) return null;

            return $"ALTER TABLE {QuotedName} MODIFY SETTING {GetCleanupSettings(after)}";
        }

        /// <summary>
        /// A merge removes the deleted rows once every part of a partition is older than the age, so in a table
        /// that gets new rows all the time it does not happen; OPTIMIZE TABLE ... FINAL CLEANUP works there.
        /// </summary>
        private static string GetCleanupSettings(TimeSpan after)
        {
            return "allow_experimental_replacing_merge_with_cleanup = 1, " +
                   "enable_replacing_merge_with_cleanup_for_min_age_to_force_merge = 1, " +
                   "min_age_to_force_merge_on_partition_only = 1, " +
                   $"min_age_to_force_merge_seconds = {(long)Math.Ceiling(after.TotalSeconds)}";
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
        /// Reads the schema of the compact write mode: the table name, then [name, isKey, type oid, type modifier, type name] per column.
        /// The type modifier and the type name are optional; the type name is there for the types that are not built in.
        /// </summary>
        public static ClickHouseTable FromSchema(string tableName, JsonElement schemaElement, bool useJsonType, bool useArrayTypes, ClickHouseTableOptions? options = null, string timestampTimeZone = "UTC")
        {
            if (schemaElement.ValueKind != JsonValueKind.Array) throw new Exception("Invalid schema - expected array");

            var schema = new List<(string, bool, uint, int, string?)>();

            var length = schemaElement.GetArrayLength();

            for (var i = 1; i < length; i++)
            {
                var columnElement = schemaElement[i];

                if (columnElement.ValueKind != JsonValueKind.Array || columnElement.GetArrayLength() < 3)
                {
                    throw new Exception("Invalid schema - expected name, key flag and type for column " + i);
                }

                schema.Add((columnElement[0].GetString() ?? string.Empty,
                            columnElement[1].GetInt32() == 1,
                            columnElement[2].GetUInt32(),
                            columnElement.GetArrayLength() > 3 && columnElement[3].TryGetInt32(out var modifier) ? modifier : -1,
                            columnElement.GetArrayLength() > 4 ? columnElement[4].GetString() : null));
            }

            return FromColumns(tableName, schema, useJsonType, useArrayTypes, options, timestampTimeZone);
        }

        /// <summary>
        /// The table of the columns of a <see cref="RowTable"/>, as <see cref="FromSchema"/> makes it from the same columns in JSON.
        /// </summary>
        public static ClickHouseTable FromRowTable(string tableName, RowTable rowTable, bool useJsonType, bool useArrayTypes, ClickHouseTableOptions? options = null, string timestampTimeZone = "UTC")
        {
            var table = FromColumns(tableName,
                                    rowTable.Columns.Select(c => (c.Name, c.IsKey, c.TypeId, c.TypeModifier, c.TypeName)),
                                    useJsonType,
                                    useArrayTypes,
                                    options,
                                    timestampTimeZone);

            table.Source = rowTable;

            return table;
        }

        private static ClickHouseTable FromColumns(string tableName,
                                                   IEnumerable<(string Name, bool IsKey, uint Oid, int TypeModifier, string? TypeName)> schema,
                                                   bool useJsonType,
                                                   bool useArrayTypes,
                                                   ClickHouseTableOptions? options,
                                                   string timestampTimeZone)
        {
            var columns = new List<ClickHouseColumn>();

            foreach (var (name, isKey, oid, typeModifier, typeName) in schema)
            {

                // a table cannot be ordered by a JSON column, so a json column of the key stays a String
                var isJson = useJsonType && !isKey && oid is (uint)PgOid.JSONOID or (uint)PgOid.JSONBOID;

                // the same for an array: a key column cannot be Nullable inside
                var elementType = useArrayTypes && !isKey ? GetArrayElementType(oid, typeModifier, timestampTimeZone) : null;

                // PostgreSQL names the array of a type after it, with an underscore: _mood is mood[].
                // The library sends its value as the PostgreSQL text, and the output turns it into an array of strings.
                var isTextArray = useArrayTypes && !isKey && elementType == null && typeName != null && typeName.StartsWith('_');

                if (isTextArray) elementType = "String";

                columns.Add(new ClickHouseColumn
                {
                    Name = name,
                    QuotedName = Quote(name),
                    IsKey = isKey,
                    TypeId = oid,
                    IsJsonb = oid == (uint)PgOid.JSONBOID,
                    Type = isJson ? "JSON" : elementType != null ? $"Array(Nullable({elementType}))" : GetType(oid, typeModifier, timestampTimeZone),
                    Kind = isTextArray ? ColumnKind.TextArray
                        : elementType != null ? (oid == (uint)PgOid.A_DATEOID ? ColumnKind.DateArray
                                                  : oid is (uint)PgOid.A_TIMESTAMPOID or (uint)PgOid.A_TIMESTAMPTZOID ? ColumnKind.TimestampArray
                                                  : ColumnKind.Array)
                        : oid == (uint)PgOid.DATEOID ? ColumnKind.Date
                        : oid is (uint)PgOid.TIMESTAMPOID or (uint)PgOid.TIMESTAMPTZOID ? ColumnKind.Timestamp
                        : isJson ? ColumnKind.Json
                        : ColumnKind.Other,
                });
            }

            return new ClickHouseTable(tableName, columns, options);
        }

        /// <summary>
        /// The ClickHouse type of the elements of a PostgreSQL array, or null for an array that stays a String:
        /// the library sends the arrays of other types (enums, user types) as their PostgreSQL text.
        /// The type modifier of an array column is that of its elements.
        /// </summary>
        /// <summary>An array type of PostgreSQL that the library writes as a JSON array.</summary>
        internal static bool IsArray(PgOid oid) => oid.IsArrayOfNumber() || oid.IsArrayOfByte() || oid.IsArrayOfBoolean() || oid.IsArrayOfText();

        internal static string? GetArrayElementType(uint oid, int typeModifier, string timestampTimeZone = "UTC")
        {
            switch ((PgOid)oid)
            {
                case PgOid.A_BOOLOID: return "Bool";
                case PgOid.A_INT2OID: return "Int16";
                case PgOid.A_INT4OID: return "Int32";
                case PgOid.A_INT8OID: return "Int64";
                case PgOid.A_OIDOID: return "UInt32";
                case PgOid.A_FLOAT4OID: return "Float32";
                case PgOid.A_FLOAT8OID: return "Float64";
                case PgOid.A_NUMERICOID: return GetNumericType(typeModifier);
                case PgOid.A_UUIDOID: return "UUID";
                case PgOid.A_DATEOID: return "Date32";
                case PgOid.A_TIMESTAMPOID: return $"DateTime64(6, '{timestampTimeZone}')";
                case PgOid.A_TIMESTAMPTZOID: return "DateTime64(6, 'UTC')";
                case PgOid.A_BYTEAOID: return "String";
                default: return ((PgOid)oid).IsArrayOfText() ? "String" : null;
            }
        }

        /// <summary>
        /// Anything without a type of its own in ClickHouse is a String with the text PostgreSQL sends.
        /// So are the arrays that are not ClickHouse arrays (see GetArrayElementType), with the JSON text, and json when the JSON type is not used.
        /// </summary>
        internal static string GetType(uint oid, int typeModifier, string timestampTimeZone = "UTC")
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
                case PgOid.TIMESTAMPOID: return $"DateTime64(6, '{timestampTimeZone}')";
                case PgOid.TIMESTAMPTZOID: return "DateTime64(6, 'UTC')";
                case PgOid.NUMERICOID: return GetNumericType(typeModifier);
                default: return "String";
            }
        }

        /// <summary>
        /// The IANA name ClickHouse knows a time zone by (a time zone of Windows is converted).
        /// </summary>
        internal static string GetTimeZoneName(TimeZoneInfo timeZone)
        {
            if (timeZone == TimeZoneInfo.Utc) return "UTC";

            if (timeZone.HasIanaId) return timeZone.Id;

            return TimeZoneInfo.TryConvertWindowsIdToIanaId(timeZone.Id, out var ianaId)
                ? ianaId
                : throw new ArgumentException($"TimestampTimeZone: the time zone {timeZone.Id} has no IANA name, which ClickHouse needs");
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
