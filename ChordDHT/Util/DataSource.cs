using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ChordDHT.Util
{
    public class DataSource : Stream
    {
        private readonly IAsyncEnumerator<byte[]>? _generator;
        private byte[] _currentBuffer = Array.Empty<byte>();
        private int _currentBufferIndex = 0;
        private long _totalBytesRead = 0;
        private long _length;

        public DataSource(IAsyncEnumerable<byte[]> generator, long? length = default)
        {
            _generator = generator.GetAsyncEnumerator();
            _length = length ?? -1;
        }

        public DataSource(IEnumerable<byte[]> generator, long? length = default)
            : this(WrapEnumerable(generator), length)
        { }

        public DataSource(IAsyncEnumerable<string> generator, long? length = default)
            : this(WrapEnumerable(generator), length)
        { }

        public DataSource(IEnumerable<string> generator, long? length = default)
            : this(WrapEnumerable(generator))
        { }


        public DataSource(byte[] chunk)
        {
            _generator = null;
            _currentBuffer = chunk;
            _length = chunk.Length;
        }

        public DataSource(string chunk)
            : this(Encoding.UTF8.GetBytes(chunk))
        { }

        public override bool CanRead => true;

        public override bool CanSeek => false;

        public override bool CanWrite => false;

        public bool HasKnownLength {
            get {
                return _length != -1;
            }
        }

        public override long Length {
            get
            {
                if (!HasKnownLength) throw new InvalidOperationException("The stream does not have a known length");
                return _length;
            }
        }

        public override long Position { get => _totalBytesRead; set => throw new NotImplementedException(); }

        public override void Flush()
        {
            // No-op in this implementation
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            return Task.Run(() => ReadAsync(buffer, offset, count)).Result;
        }

        public override async Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            int bytesRead = 0;

            while (count > 0)
            {
                if (_currentBufferIndex >= _currentBuffer.Length)
                {
                    if (_generator == null)
                    {
                        break; // End of generator
                    }

                    if (!await _generator.MoveNextAsync())
                    {
                        break; // End of generator
                    }

                    _currentBuffer = _generator.Current;
                    _currentBufferIndex = 0;
                }

                int bytesToCopy = Math.Min(count, _currentBuffer.Length - _currentBufferIndex);
                Array.Copy(_currentBuffer, _currentBufferIndex, buffer, offset, bytesToCopy);

                bytesRead += bytesToCopy;
                _currentBufferIndex += bytesToCopy;
                offset += bytesToCopy;
                count -= bytesToCopy;
                _totalBytesRead += bytesRead;
            }

            return bytesRead;
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            throw new NotImplementedException();
        }

        public override void SetLength(long value)
        {
            throw new NotImplementedException();
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            throw new NotImplementedException();
        }

        public static DataSource Empty = new DataSource(Array.Empty<byte>());

        private static async IAsyncEnumerable<byte[]> WrapEnumerable(IAsyncEnumerable<string> generator)
        {
            await foreach (string str in generator)
            {
                yield return Encoding.UTF8.GetBytes(str);
            }
        }

        private static async IAsyncEnumerable<byte[]> WrapEnumerable(IEnumerable<byte[]> generator)
        {
            foreach (byte[] chunk in generator)
            {
                yield return chunk;
                await Task.Yield();
            }
        }

        private static async IAsyncEnumerable<byte[]> WrapEnumerable(IEnumerable<string> generator)
        {
            foreach (string str in generator)
            {
                yield return Encoding.UTF8.GetBytes(str);
                await Task.Yield();
            }
        }
    }
}
