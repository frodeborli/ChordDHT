using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Text.Json.Serialization;
using System.Threading.Tasks;

namespace ChordDHT.DHT
{
    public class StoredItem : IStoredItem
    {
        [JsonPropertyName("content_type")]
        public string ContentType { get; private set; }

        [JsonPropertyName("data")]
        public byte[] Data { get; private set; }

        [JsonPropertyName("datetime")]
        public DateTime CreatedDate { get; private set; }

        public StoredItem(string contentType, byte[] data)
        {
            ContentType = contentType;
            Data = data;
            CreatedDate = DateTime.Now;
        }

        public StoredItem(string data)
        {
            ContentType = "text/plain; charset=utf-8";
            Data = Encoding.UTF8.GetBytes(data);
            CreatedDate = DateTime.Now;
        }

        public static async Task<StoredItem> CreateFrom(HttpResponseMessage response)
        {
            if (response.Content.Headers.ContentType?.MediaType == null)
            {
                throw new InvalidOperationException("Response has no Content-Type header");
            }
            var contentType = response.Content.Headers.ContentType.MediaType;
            var body = await response.Content.ReadAsByteArrayAsync();
            if (body == null)
            {
                throw new InvalidOperationException("Response has no body");
            }
            else
            {
                return new StoredItem(contentType, body);
            }
        }


        public override string ToString()
        {
            if (ContentType == "text/plain")
            {
                return Encoding.UTF8.GetString(Data);
            }
            return $"data:{ContentType};base64,{Convert.ToBase64String(Data)}";
        }
    }
}
