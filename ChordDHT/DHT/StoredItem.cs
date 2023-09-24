using System;
using System.Collections.Generic;
using System.Linq;
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

        public StoredItem(string contentType, byte[] data)
        {
            this.ContentType = contentType;
            this.Data = data;
        }

        public StoredItem(string data)
        {
            this.ContentType = "text/plain; charset=utf-8";
            this.Data = Encoding.UTF8.GetBytes(data);
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
