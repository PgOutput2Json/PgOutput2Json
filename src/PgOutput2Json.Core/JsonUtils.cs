using System.Text;

namespace Pg2Rabbit.Core
{
    internal static class JsonUtils
    {
        internal static void WriteText(StringBuilder builder, TextReader textReader)
        {
            builder.Append('"');

            EscapeJson(builder, textReader);

            builder.Append('"');
        }

        internal static void WriteNumber(StringBuilder stringBuilder, TextReader textReader)
        {
            // we may have to return to the original length in case of NaN, Infinity, -Infinity...
            var originalLength = stringBuilder.Length;

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
                    stringBuilder.Append((char)c);
                }
                else
                {
                    stringBuilder.Length = originalLength;
                    stringBuilder.Append("null");
                    break;
                }
            }
        }

        internal static void WriteBoolean(StringBuilder stringBuilder, TextReader textReader)
        {
            int c;
            if ((c = textReader.Read()) != -1)
            {
                stringBuilder.Append(((char)c) == 't' ? "true" : "false");
            }
        }

        internal static void WriteByte(StringBuilder builder, TextReader textReader)
        {
            /* string is "\x54617069727573", start after "\x" */
            textReader.Read();
            textReader.Read();

            builder.Append('"');

            int c;
            while ((c = textReader.Read()) != -1)
            {
                builder.Append((char)c);
            }

            builder.Append('"');
        }

        private static void EscapeJson(StringBuilder builder, TextReader textReader)
        {
            int c;
            while ((c = textReader.Read()) != -1)
            {
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
        }
    }
}
