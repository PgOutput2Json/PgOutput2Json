using System;
using System.Text;

namespace PgOutput2Json
{
    internal static class JsonUtils
    {
        internal static int WriteText(StringBuilder jsonBuilder, ReadOnlySpan<char> value)
        {
            jsonBuilder.Append('"');

            var hash = EscapeText(jsonBuilder, value);

            jsonBuilder.Append('"');

            return hash;
        }

        internal static int WriteNumber(StringBuilder jsonBuilder, ReadOnlySpan<char> value)
        {
            // we may have to return to the original length in case of NaN, Infinity, -Infinity...
            var originalLength = jsonBuilder.Length;

            int hash = 0;

            foreach (var c in value)
            {
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
                    jsonBuilder.Append("0");
                    return 0;
                }
            }

            return hash;
        }

        internal static int WriteBoolean(StringBuilder jsonBuilder, ReadOnlySpan<char> value)
        {
            if (value == "t")
            {
                jsonBuilder.Append("true");
                return 't';
            }

            jsonBuilder.Append("false");
            return 'f';
        }

        internal static int WriteByte(StringBuilder jsonBuilder, ReadOnlySpan<char> value)
        {
            jsonBuilder.Append('"');

            int hash = 0;

            /* string is "\x54617069727573", start after "\x" */
            for (var i = 2; i < value.Length; i++)
            {
                var c = value[i];

                hash ^= c;
                jsonBuilder.Append(c);
            }

            jsonBuilder.Append('"');
            return hash;
        }

        internal static int WriteArrayOfNumber(StringBuilder jsonBuilder, string value)
        {
            return WriteSimpleArray(jsonBuilder, value, (b, v, s, l) => WriteNumber(b, v.AsSpan(s, l)));
        }

        internal static int WriteArrayOfByte(StringBuilder jsonBuilder, string value)
        {
            return WriteSimpleArray(jsonBuilder, value, (b, v, s, l) => WriteByte(b, v.AsSpan(s, l)));
        }

        internal static int WriteArrayOfBoolean(StringBuilder jsonBuilder, string value)
        {
            return WriteSimpleArray(jsonBuilder, value, (b, v, s, l) => WriteBoolean(b, v.AsSpan(s, l)));
        }

        internal static int WriteArrayOfText(StringBuilder jsonBuilder, ReadOnlySpan<char> value)
        {
            var hash = 0;
            var inString = false;
            var escaped = false;
            var inValue = false;

            for (var i = 0; i < value.Length; i++)
            {
                var c = value[i];

                if (escaped)
                {
                    escaped = false;

                    hash ^= c;
                    EscapeChar(jsonBuilder, c);
                    continue;
                }

                if (c == '\\')
                {
                    escaped = true;
                    continue;
                }

                if (!inString && c == '\"')
                {
                    inString = true;
                    continue;
                }

                if (inString && c == '\"')
                {
                    inString = false;
                    continue;
                }

                if (!inString)
                {
                    if (c == '{')
                    {
                        jsonBuilder.Append('[');
                        continue;
                    }

                    if (c == '}' || c == ',')
                    {
                        inValue = false;
                        jsonBuilder.Append('\"');
                        jsonBuilder.Append(c == '}' ? ']' : c);
                        continue;
                    }
                }

                if (!inValue)
                {
                    inValue = true;
                    jsonBuilder.Append('\"');
                }

                hash ^= c;
                EscapeChar(jsonBuilder, c);
            }

            return hash;
        }

        internal static int EscapeText(StringBuilder jsonBuilder, ReadOnlySpan<char> value)
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
                        hash ^= writer(jsonBuilder, value, start, len);
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
