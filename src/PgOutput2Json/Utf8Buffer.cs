using System;
using System.Text;

namespace PgOutput2Json
{
    /// <summary>
    /// A buffer reused for the UTF-8 bytes of one message at a time, so that a message is parsed without
    /// a string of it, and without the byte array that parsing a string rents and returns.
    /// The bytes are valid until the next call.
    /// </summary>
    internal sealed class Utf8Buffer
    {
        private readonly Encoder _encoder = Encoding.UTF8.GetEncoder();

        private byte[] _buffer = new byte[4096];

        public ReadOnlyMemory<byte> Encode(StringBuilder text)
        {
            var maxLength = Encoding.UTF8.GetMaxByteCount(text.Length);

            if (_buffer.Length < maxLength) _buffer = new byte[Math.Max(maxLength, _buffer.Length * 2)];

            // an encoder, because a chunk of the StringBuilder can end in the middle of a surrogate pair
            _encoder.Reset();

            var length = 0;

            foreach (var chunk in text.GetChunks())
            {
                length += _encoder.GetBytes(chunk.Span, _buffer.AsSpan(length), flush: false);
            }

            length += _encoder.GetBytes(ReadOnlySpan<char>.Empty, _buffer.AsSpan(length), flush: true);

            return _buffer.AsMemory(0, length);
        }
    }
}
