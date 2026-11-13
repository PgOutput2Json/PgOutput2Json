using System;
using System.Text;

namespace PgOutput2Json
{
    public static class JsonUtils
    {
        public static int WriteText(StringBuilder jsonBuilder, ReadOnlySpan<char> value)
        {
            jsonBuilder.Append('"');

            var hash = EscapeText(jsonBuilder, value);

            jsonBuilder.Append('"');

            return hash;
        }

        public static int WriteNumber(StringBuilder jsonBuilder, string value)
        {
            return WriteNumber(jsonBuilder, value, 0, value.Length);
        }

        public static int WriteNumber(StringBuilder jsonBuilder, string value, int start, int len)
        {
            // JSON has no NaN, Infinity or -Infinity: they are written as null, so we may have to return to the original length
            var originalLength = jsonBuilder.Length;

            int hash = 0;

            for (var i = start; i < start + len; i++)
            {
                var c = value[i];

                // allowed chars "0123456789+-eE."
                if ((c >= '0' && c <= '9')
                     || c == '+'
                     || c == '-'
                     || c == 'e'
                     || c == 'E'
                     || c == '.')
                {
                    hash ^= c;
                    jsonBuilder.Append(c);
                }
                else
                {
                    jsonBuilder.Length = originalLength;
                    jsonBuilder.Append("null");
                    return 0;
                }
            }

            return hash;
        }

        public static int WriteBoolean(StringBuilder jsonBuilder, string value)
        {
            return WriteBoolean(jsonBuilder, value, 0, value.Length);
        }

        public static int WriteBoolean(StringBuilder jsonBuilder, string value, int start, int len)
        {
            if (value.Length > start && value[start] == 't')
            {
                jsonBuilder.Append("true");
                return 't';
            }

            jsonBuilder.Append("false");
            return 'f';
        }

        public static int WriteByte(StringBuilder jsonBuilder, string value)
        {
            return WriteByte(jsonBuilder, value, 0, value.Length);
        }

        public static int WriteByte(StringBuilder jsonBuilder, string value, int start, int len)
        {
            jsonBuilder.Append('"');

            int hash = 0;

            /* string is "\x54617069727573", start after "\x" */
            for (var i = start + 2; i < start + len; i++)
            {
                var c = value[i];

                hash ^= c;
                jsonBuilder.Append(c);
            }

            jsonBuilder.Append('"');
            return hash;
        }

        public static int WriteArrayOfNumber(StringBuilder jsonBuilder, string value)
        {
            return WriteSimpleArray(jsonBuilder, value, WriteNumber);
        }

        /// <summary>
        /// Writes a bytea array, e.g. {"\\x01",NULL,"\\xff"}: PostgreSQL quotes the elements, because of the backslash.
        /// </summary>
        public static int WriteArrayOfByte(StringBuilder jsonBuilder, string value)
        {
            var hash = 0;
            var inString = false;

            for (var i = 0; i < value.Length; i++)
            {
                var c = value[i];

                if (c == '"')
                {
                    inString = !inString;
                    jsonBuilder.Append('"');
                }
                else if (inString)
                {
                    // the element is \\x followed by the hex digits
                    if (c == '\\' || c == 'x') continue;

                    hash ^= c;
                    jsonBuilder.Append(c);
                }
                else if (c == '{')
                {
                    jsonBuilder.Append('[');
                }
                else if (c == '}')
                {
                    jsonBuilder.Append(']');
                }
                else if (c == ',')
                {
                    jsonBuilder.Append(',');
                }
                else if (c == 'N')
                {
                    jsonBuilder.Append("null");
                    i += 3; // NULL
                }
            }

            return hash;
        }

        public static int WriteArrayOfBoolean(StringBuilder jsonBuilder, string value)
        {
            return WriteSimpleArray(jsonBuilder, value, WriteBoolean);
        }

        /// <summary>
        /// Writes a text array, e.g. {a,"b,c",NULL,""}. PostgreSQL quotes an element that is empty, equals NULL,
        /// or contains a special character, so an unquoted NULL is a null and everything else is a string.
        /// </summary>
        public static int WriteArrayOfText(StringBuilder jsonBuilder, ReadOnlySpan<char> value)
        {
            var hash = 0;
            var inString = false;
            var escaped = false;
            var unquotedStart = -1;

            for (var i = 0; i < value.Length; i++)
            {
                var c = value[i];

                if (inString)
                {
                    if (escaped)
                    {
                        escaped = false;
                    }
                    else if (c == '\\')
                    {
                        escaped = true;
                        continue;
                    }
                    else if (c == '"')
                    {
                        inString = false;
                        jsonBuilder.Append('"');
                        continue;
                    }

                    hash ^= c;
                    EscapeChar(jsonBuilder, c);
                    continue;
                }

                if (c == '"')
                {
                    inString = true;
                    jsonBuilder.Append('"');
                    continue;
                }

                if (c == '{' || c == '}' || c == ',')
                {
                    if (unquotedStart >= 0)
                    {
                        var element = value[unquotedStart..i];

                        if (element.SequenceEqual("NULL"))
                        {
                            jsonBuilder.Append("null");
                        }
                        else
                        {
                            hash ^= WriteText(jsonBuilder, element);
                        }

                        unquotedStart = -1;
                    }

                    jsonBuilder.Append(c == '{' ? '[' : c == '}' ? ']' : ',');
                    continue;
                }

                if (unquotedStart < 0) unquotedStart = i;
            }

            return hash;
        }

        public static int EscapeText(StringBuilder jsonBuilder, ReadOnlySpan<char> value)
        {
            int hash = 0;

            foreach (var c in value)
            {
                hash ^= c;
                EscapeChar(jsonBuilder, c);
            }

            return hash;
        }

        private static void EscapeChar(StringBuilder jsonBuilder, char c)
        {
            switch (c)
            {
                case '\b':
                    jsonBuilder.Append("\\b");
                    break;
                case '\f':
                    jsonBuilder.Append("\\f");
                    break;
                case '\n':
                    jsonBuilder.Append("\\n");
                    break;
                case '\r':
                    jsonBuilder.Append("\\r");
                    break;
                case '\t':
                    jsonBuilder.Append("\\t");
                    break;
                case '"':
                    jsonBuilder.Append("\\\"");
                    break;
                case '\\':
                    jsonBuilder.Append("\\\\");
                    break;
                default:
                    if (c < ' ')
                    {
                        jsonBuilder.Append("\\u");
                        jsonBuilder.Append(((int)c).ToString("x04"));
                    }
                    else
                    {
                        jsonBuilder.Append(c);
                    }
                    break;
            }
        }

        private static int WriteSimpleArray(StringBuilder jsonBuilder, string value, 
            Func<StringBuilder, string, int, int, int> writer)
        {
            var start = -1;
            var hash = 0;

            for (var i = 0; i < value.Length; i++)
            {
                var c = value[i];

                if (c == '{')
                {
                    jsonBuilder.Append('[');
                    continue;
                }
                
                if (c == '}' || c == ',')
                {
                    if (start >= 0)
                    {
                        var len = i - start;

                        if (value.AsSpan(start, len).SequenceEqual("NULL"))
                        {
                            jsonBuilder.Append("null");
                        }
                        else
                        {
                            hash ^= writer(jsonBuilder, value, start, len);
                        }

                        start = -1;
                    }

                    jsonBuilder.Append(c == '}' ? ']' : c);
                    continue;
                }

                if (start < 0) start = i;
            }

            return hash;
        }
    }
}
