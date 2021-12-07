using System.Text;

namespace PgOutput2Json.Core
{
    internal static class JsonUtils
    {
        private const int _initHash = 0x12345678;

        internal static int WriteText(StringBuilder jsonBuilder, TextReader textReader)
        {
            jsonBuilder.Append('"');

            var hash = EscapeJson(jsonBuilder, textReader);

            jsonBuilder.Append('"');

            return hash;
        }

        internal static int WriteNumber(StringBuilder jsonBuilder, TextReader textReader)
        {
            // we may have to return to the original length in case of NaN, Infinity, -Infinity...
            var originalLength = jsonBuilder.Length;

            int hash = _initHash;

            int c;
            while ((c = textReader.Read()) != -1)
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
                    jsonBuilder.Append((char)c);
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

        internal static int WriteBoolean(StringBuilder jsonBuilder, TextReader textReader)
        {
            int hash = 0;

            int c;
            if ((c = textReader.Read()) != -1)
            {
                if (c == 't')
                {
                    hash = 1;
                    jsonBuilder.Append("true");
                }
                else
                {
                    jsonBuilder.Append("false");
                }
            }

            return hash;
        }

        internal static int WriteByte(StringBuilder jsonBuilder, TextReader textReader)
        {
            /* string is "\x54617069727573", start after "\x" */
            textReader.Read();
            textReader.Read();

            jsonBuilder.Append('"');

            int hash = _initHash;

            int c;
            while ((c = textReader.Read()) != -1)
            {
                hash ^= c;
                jsonBuilder.Append((char)c);
            }

            jsonBuilder.Append('"');
            return hash;
        }

        private static int EscapeJson(StringBuilder jsonBuilder, TextReader textReader)
        {
            int hash = _initHash;

            int c;
            while ((c = textReader.Read()) != -1)
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
                            jsonBuilder.Append(c.ToString("x04"));
                        }
                        else
                        {
                            jsonBuilder.Append((char)c);
                        }
                        break;
                }
            }

            return hash;
        }
    }
}
