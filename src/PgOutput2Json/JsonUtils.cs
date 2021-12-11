using System.IO;
using System.Text;

namespace PgOutput2Json
{
    internal static class JsonUtils
    {
        internal static int WriteText(StringBuilder jsonBuilder, string value)
        {
            jsonBuilder.Append('"');

            var hash = EscapeJson(jsonBuilder, value);

            jsonBuilder.Append('"');

            return hash;
        }

        internal static int WriteNumber(StringBuilder jsonBuilder, string value)
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
                    jsonBuilder.Append("null");
                    return 0;
                }
            }

            return hash;
        }

        internal static int WriteBoolean(StringBuilder jsonBuilder, string value)
        {
            if (value == "t")
            {
                jsonBuilder.Append("true");
                return 't';
            }

            jsonBuilder.Append("false");
            return 'f';
        }

        internal static int WriteByte(StringBuilder jsonBuilder, string value)
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

        public static int EscapeJson(StringBuilder jsonBuilder, string value)
        {
            int hash = 0;

            foreach (var c in value)
            {
                hash ^= c;

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

            return hash;
        }
    }
}
