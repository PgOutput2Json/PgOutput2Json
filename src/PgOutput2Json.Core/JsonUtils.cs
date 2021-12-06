using System.Text;

namespace PgOutput2Json.Core
{
    internal static class JsonUtils
    {
        private const int _initHash = 0x12345678;

        internal static int WriteText(StringBuilder builder, TextReader textReader)
        {
            builder.Append('"');

            var hash = EscapeJson(builder, textReader);

            builder.Append('"');

            return hash;
        }

        internal static int WriteNumber(StringBuilder stringBuilder, TextReader textReader)
        {
            // we may have to return to the original length in case of NaN, Infinity, -Infinity...
            var originalLength = stringBuilder.Length;

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
                    stringBuilder.Append((char)c);
                }
                else
                {
                    stringBuilder.Length = originalLength;
                    stringBuilder.Append("null");
                    return 0;
                }
            }

            return hash;
        }

        internal static int WriteBoolean(StringBuilder stringBuilder, TextReader textReader)
        {
            int hash = 0;

            int c;
            if ((c = textReader.Read()) != -1)
            {
                if (c == 't')
                {
                    hash = 1;
                    stringBuilder.Append("true");
                }
                else
                {
                    stringBuilder.Append("false");
                }
            }

            return hash;
        }

        internal static int WriteByte(StringBuilder builder, TextReader textReader)
        {
            /* string is "\x54617069727573", start after "\x" */
            textReader.Read();
            textReader.Read();

            builder.Append('"');

            int hash = _initHash;

            int c;
            while ((c = textReader.Read()) != -1)
            {
                hash ^= c;
                builder.Append((char)c);
            }

            builder.Append('"');
            return hash;
        }

        private static int EscapeJson(StringBuilder builder, TextReader textReader)
        {
            int hash = _initHash;

            int c;
            while ((c = textReader.Read()) != -1)
            {
                hash ^= c;

                switch (c)
                {
                    case '\b':
                        builder.Append("\\b");
                        break;
                    case '\f':
                        builder.Append("\\f");
                        break;
                    case '\n':
                        builder.Append("\\n");
                        break;
                    case '\r':
                        builder.Append("\\r");
                        break;
                    case '\t':
                        builder.Append("\\t");
                        break;
                    case '"':
                        builder.Append("\\\"");
                        break;
                    case '\\':
                        builder.Append("\\\\");
                        break;
                    default:
                        if (c < ' ')
                        {
                            builder.Append("\\u");
                            builder.Append(c.ToString("x04"));
                        }
                        else
                        {
                            builder.Append((char)c);
                        }
                        break;
                }
            }

            return hash;
        }
    }
}
