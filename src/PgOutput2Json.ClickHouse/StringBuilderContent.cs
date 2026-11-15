using System;
using System.Buffers;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace PgOutput2Json.ClickHouse
{
    /// <summary>
    /// The rows of an insert, written to the request as UTF-8 straight from the StringBuilder they were collected in:
    /// a batch is megabytes, and a string and a byte array of it would be two more copies on the large object heap.
    /// </summary>
    internal sealed class StringBuilderContent : HttpContent
    {
        private const int BufferSize = 16 * 1024;

        private readonly StringBuilder _text;

        public StringBuilderContent(StringBuilder text)
        {
            _text = text;

            Headers.ContentType = new MediaTypeHeaderValue("text/plain") { CharSet = "utf-8" };
        }

        protected override Task SerializeToStreamAsync(Stream stream, TransportContext? context)
        {
            return SerializeToStreamAsync(stream, context, CancellationToken.None);
        }

        protected override async Task SerializeToStreamAsync(Stream stream, TransportContext? context, CancellationToken cancellationToken)
        {
            // an encoder, because a chunk can end in the middle of a surrogate pair
            var encoder = Encoding.UTF8.GetEncoder();

            var buffer = ArrayPool<byte>.Shared.Rent(BufferSize);

            try
            {
                foreach (var chunk in _text.GetChunks())
                {
                    var chars = chunk;

                    while (!chars.IsEmpty)
                    {
                        // at most a third of the buffer, so that the bytes of the chars always fit
                        var take = Math.Min(chars.Length, BufferSize / 3);

                        var bytes = encoder.GetBytes(chars.Span[..take], buffer, flush: false);

                        await stream.WriteAsync(buffer.AsMemory(0, bytes), cancellationToken).ConfigureAwait(false);

                        chars = chars[take..];
                    }
                }

                var last = encoder.GetBytes(ReadOnlySpan<char>.Empty, buffer, flush: true);

                if (last > 0) await stream.WriteAsync(buffer.AsMemory(0, last), cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(buffer);
            }
        }

        // sent chunked: counting the bytes would encode the rows twice
        protected override bool TryComputeLength(out long length)
        {
            length = 0;
            return false;
        }
    }
}
