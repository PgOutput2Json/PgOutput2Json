using System;
using System.Globalization;
using System.Text;
using System.Text.Json;

namespace PgOutput2Json.Sqlite
{
    internal enum SqliteValueKind
    {
        Null,
        Number,
        True,
        False,
        String,

        /// <summary>An array: bound as its JSON text.</summary>
        Json,

        /// <summary>A large value (TOAST) that the update did not change.</summary>
        Unchanged,
    }

    /// <summary>
    /// A value of a change: a value of the compact JSON, or the text PostgreSQL sends for an <see cref="IRowPublisher"/> row,
    /// which then has the kind of the JSON value the library would write for it.
    /// </summary>
    internal readonly struct SqliteValue
    {
        private const string ToastValue = "__TOAST__";

        private readonly JsonElement _element;
        private readonly string? _text;
        private readonly bool _isText;

        public SqliteValueKind Kind { get; }

        private SqliteValue(JsonElement element, SqliteValueKind kind)
        {
            _element = element;
            Kind = kind;
        }

        private SqliteValue(string? text, SqliteValueKind kind)
        {
            _text = text;
            _isText = true;
            Kind = kind;
        }

        public static SqliteValue FromJson(JsonElement element)
        {
            var kind = element.ValueKind switch
            {
                JsonValueKind.Null or JsonValueKind.Undefined => SqliteValueKind.Null,
                JsonValueKind.Number => SqliteValueKind.Number,
                JsonValueKind.True => SqliteValueKind.True,
                JsonValueKind.False => SqliteValueKind.False,
                JsonValueKind.String => element.ValueEquals(ToastValue) ? SqliteValueKind.Unchanged : SqliteValueKind.String,
                _ => SqliteValueKind.Json,
            };

            return new SqliteValue(element, kind);
        }

        public static SqliteValue FromRow(RowValue value, uint typeId, StringBuilder json)
        {
            if (value.Kind == RowValueKind.Null) return new SqliteValue(null, SqliteValueKind.Null);
            if (value.Kind == RowValueKind.Unchanged) return new SqliteValue(null, SqliteValueKind.Unchanged);

            var text = value.Text!;
            var oid = (PgOid)typeId;

            if (oid.IsNumber())
            {
                // JSON has no NaN and no infinity: the library writes them as null
                foreach (var c in text)
                {
                    if (!(c is >= '0' and <= '9' or '+' or '-' or 'e' or 'E' or '.')) return new SqliteValue(null, SqliteValueKind.Null);
                }

                return new SqliteValue(text, SqliteValueKind.Number);
            }

            if (oid.IsBoolean()) return new SqliteValue(null, text.Length > 0 && text[0] == 't' ? SqliteValueKind.True : SqliteValueKind.False);

            // the hex digits, after \x
            if (oid.IsByte()) return new SqliteValue(text.Length >= 2 ? text[2..] : "", SqliteValueKind.String);

            if (oid.IsArrayOfNumber() || oid.IsArrayOfByte() || oid.IsArrayOfBoolean() || oid.IsArrayOfText())
            {
                JsonUtils.WriteValue(json.Clear(), oid, text);

                return new SqliteValue(json.ToString(), SqliteValueKind.Json);
            }

            return new SqliteValue(text, SqliteValueKind.String);
        }

        /// <summary>The text of a String, a Number as written, or the JSON of an array.</summary>
        public string Text => _isText ? _text! : Kind == SqliteValueKind.String ? _element.GetString()! : _element.GetRawText();

        public bool TryGetInt64(out long value) => _isText
            ? long.TryParse(_text, NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture, out value)
            : _element.TryGetInt64(out value);

        public bool TryGetDecimal(out decimal value) => _isText
            ? decimal.TryParse(_text, NumberStyles.Float, CultureInfo.InvariantCulture, out value)
            : _element.TryGetDecimal(out value);

        /// <summary>The same value, as the library would write it.</summary>
        public bool IsSameAs(SqliteValue other)
        {
            if (Kind != other.Kind) return false;

            return Kind is SqliteValueKind.Null or SqliteValueKind.True or SqliteValueKind.False or SqliteValueKind.Unchanged
                || (_isText && other._isText ? _text == other._text : GetRawText() == other.GetRawText());
        }

        private string GetRawText() => _isText ? _text! : _element.GetRawText();
    }
}
