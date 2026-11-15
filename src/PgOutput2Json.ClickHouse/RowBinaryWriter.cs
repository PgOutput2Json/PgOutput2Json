using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Generic;
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
    /// The rows of an insert in the RowBinary format, collected in segments that stay below the size of the
    /// large object heap. The segments are kept for the next batch.
    /// </summary>
    internal sealed class RowBinaryWriter
    {
        private const int SegmentSize = 64 * 1024;

        private readonly List<byte[]> _segments = [new byte[SegmentSize]];

        // the bytes written in each segment before the current one: a value that does not fit starts a new segment
        private readonly List<int> _used = [];

        // the segment being written, and the position in it
        private int _segment;
        private int _position;

        public long Length { get; private set; }

        public void Clear()
        {
            _used.Clear();
            _segment = 0;
            _position = 0;
            Length = 0;
        }

        /// <summary>
        /// The written bytes, segment by segment.
        /// </summary>
        public IEnumerable<ReadOnlyMemory<byte>> GetSegments()
        {
            for (var i = 0; i < _segment; i++) yield return _segments[i].AsMemory(0, _used[i]);

            yield return _segments[_segment].AsMemory(0, _position);
        }

        public void WriteByte(byte value) => GetSpan(1)[0] = value;

        public void WriteInt16(short value) => BinaryPrimitives.WriteInt16LittleEndian(GetSpan(2), value);

        public void WriteInt32(int value) => BinaryPrimitives.WriteInt32LittleEndian(GetSpan(4), value);

        public void WriteUInt32(uint value) => BinaryPrimitives.WriteUInt32LittleEndian(GetSpan(4), value);

        public void WriteInt64(long value) => BinaryPrimitives.WriteInt64LittleEndian(GetSpan(8), value);

        public void WriteUInt64(ulong value) => BinaryPrimitives.WriteUInt64LittleEndian(GetSpan(8), value);

        public void WriteFloat32(float value) => BinaryPrimitives.WriteSingleLittleEndian(GetSpan(4), value);

        public void WriteFloat64(double value) => BinaryPrimitives.WriteDoubleLittleEndian(GetSpan(8), value);

        /// <summary>
        /// A length: LEB128, 7 bits a byte, the lowest first.
        /// </summary>
        public void WriteVarUInt(ulong value)
        {
            while (value >= 0x80)
            {
                WriteByte((byte)(value | 0x80));
                value >>= 7;
            }

            WriteByte((byte)value);
        }

        /// <summary>
        /// A fixed number of bytes, which must fit in a segment.
        /// </summary>
        public void WriteFixed(ReadOnlySpan<byte> value) => value.CopyTo(GetSpan(value.Length));

        public void WriteBytes(ReadOnlySpan<byte> value)
        {
            while (!value.IsEmpty)
            {
                if (_position == SegmentSize) NextSegment();

                var length = Math.Min(SegmentSize - _position, value.Length);

                value[..length].CopyTo(_segments[_segment].AsSpan(_position));

                _position += length;
                Length += length;

                value = value[length..];
            }
        }

        /// <summary>
        /// A String: its length in bytes, then its UTF-8.
        /// </summary>
        public void WriteString(ReadOnlySpan<char> value)
        {
            var length = Encoding.UTF8.GetByteCount(value);

            WriteVarUInt((ulong)length);

            if (length <= SegmentSize - _position)
            {
                Encoding.UTF8.GetBytes(value, GetSpan(length));
                return;
            }

            // longer than what is left of the segment
            var bytes = ArrayPool<byte>.Shared.Rent(length);

            try
            {
                WriteBytes(bytes.AsSpan(0, Encoding.UTF8.GetBytes(value, bytes)));
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(bytes);
            }
        }

        public void WriteString(ReadOnlySpan<byte> utf8)
        {
            WriteVarUInt((ulong)utf8.Length);
            WriteBytes(utf8);
        }

        /// <summary>
        /// Room for a value of <paramref name="size"/> bytes (at most 32), in one segment.
        /// </summary>
        private Span<byte> GetSpan(int size)
        {
            if (SegmentSize - _position < size) NextSegment();

            var span = _segments[_segment].AsSpan(_position, size);

            _position += size;
            Length += size;

            return span;
        }

        private void NextSegment()
        {
            _used.Add(_position);

            _segment++;
            _position = 0;

            if (_segment == _segments.Count) _segments.Add(new byte[SegmentSize]);
        }
    }

    /// <summary>
    /// The body of an insert in the RowBinary format.
    /// </summary>
    internal sealed class RowBinaryContent : HttpContent
    {
        private readonly RowBinaryWriter _rows;

        public RowBinaryContent(RowBinaryWriter rows)
        {
            _rows = rows;

            Headers.ContentType = new MediaTypeHeaderValue("application/octet-stream");
        }

        protected override Task SerializeToStreamAsync(Stream stream, TransportContext? context)
        {
            return SerializeToStreamAsync(stream, context, CancellationToken.None);
        }

        protected override async Task SerializeToStreamAsync(Stream stream, TransportContext? context, CancellationToken cancellationToken)
        {
            foreach (var segment in _rows.GetSegments())
            {
                await stream.WriteAsync(segment, cancellationToken).ConfigureAwait(false);
            }
        }

        protected override bool TryComputeLength(out long length)
        {
            length = _rows.Length;
            return true;
        }
    }
}
