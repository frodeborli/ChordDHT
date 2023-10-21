using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Fubber
{
    public class BodyParser
    {
        private HttpContext Context;

        private string? _Body = null;
        private object? _ParsedBody = null;
        private int MaxBodySize = 1024 * 1024;

        public async Task<string> GetBodyAsync()
        {
            if (_Body == null)
            {
                if (!Context.Request.HasEntityBody)
                {
                    throw new InvalidOperationException("Request has no entity body");
                }
                if (!Context.Request.InputStream.CanRead)
                {
                    throw new InvalidOperationException("Request input stream is not readable");
                }
                if (Context.Request.ContentLength64 > MaxBodySize)
                {
                    throw new InvalidOperationException("Request body too large");
                }

                // Read until at most MaxBodySize
                StringBuilder stringBuilder = new StringBuilder();
                byte[] buffer = new byte[8192];
                int bytesRead = 0;
                int totalBytesRead = 0;

                do
                {
                    bytesRead = await Context.Request.InputStream.ReadAsync(buffer, 0, buffer.Length);
                    totalBytesRead += bytesRead;

                    if (totalBytesRead > MaxBodySize)
                    {
                        throw new InvalidOperationException("Request body too large");
                    }

                    stringBuilder.Append(Encoding.UTF8.GetString(buffer, 0, bytesRead));
                }
                while (bytesRead > 0);

                _Body = stringBuilder.ToString();
            }
            return _Body;
        }

        public BodyParser(HttpContext context)
        {
            Context = context;
        }

        public async Task ParseAsync()
        {
            if (_ParsedBody != null)
            {
                // Body has already been parsed
                return;
            }
            var body = await GetBodyAsync();
        }

        public object? this[string fieldName]
        {
            get
            {
                AssertParsed();
                var property = _ParsedBody!.GetType().GetProperty(fieldName);
                if (property == null)
                {
                    return null;
                }
                return property.GetValue(_ParsedBody);
            }
        }

        private void AssertParsed()
        {
            if (_ParsedBody == null)
            {
                throw new InvalidOperationException("Body has not been parsed. Invoke ParseAsync() before accessing the fields.");
            }
        }
    }
}
