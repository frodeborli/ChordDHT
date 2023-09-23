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
        public string contentType { get; private set; }

        [JsonPropertyName("data")]
        public byte[] data { get; private set; }

        public StoredItem(string contentType, byte[] data)
        {
            this.contentType = contentType;
            this.data = data;
        }

        public StoredItem(string data)
        {
            this.contentType = "text/plain; charset=utf-8";
            this.data = Encoding.UTF8.GetBytes(data);
        }

        public override string ToString()
        {
            if (contentType == "text/plain")
            {
                return Encoding.UTF8.GetString(data);
            }
            return $"data:{contentType};base64,{Convert.ToBase64String(data)}";
        }
    }
}
