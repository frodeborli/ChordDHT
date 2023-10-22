using ChordProtocol;
using Fubber;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading.Tasks;

namespace ChordDHT.ChordProtocol
{
    public abstract class Message : IMessage
    {
        public Node? Sender { get; set; }

        public Node Receiver { get; set; }

        public string ToJson()
        {
            var originalJson = JsonSerializer.Serialize(this, this.GetType());
            var jsonDocument = JsonDocument.Parse(originalJson);

            Dictionary<string, JsonElement> jsonObject = new Dictionary<string, JsonElement>();
            foreach (JsonProperty property in jsonDocument.RootElement.EnumerateObject())
            {
                jsonObject.Add(property.Name, property.Value.Clone());
            }

            using (var jsonDoc = JsonDocument.Parse($"{{\"$type\": \"{GetType().AssemblyQualifiedName}\"}}"))
            {
                jsonObject["$type"] = jsonDoc.RootElement.GetProperty("$type").Clone();
            }

            return JsonSerializer.Serialize(jsonObject);
        }

        public static IMessage? FromJson(string json)
        {
            var jsonDocument = JsonDocument.Parse(json);
            Dictionary<string, JsonElement> jsonObject = new Dictionary<string, JsonElement>();

            foreach (JsonProperty property in jsonDocument.RootElement.EnumerateObject())
            {
                jsonObject.Add(property.Name, property.Value.Clone());
            }

            if (!jsonObject.ContainsKey("$type"))
            {
                throw new InvalidDataException("$type annotation missing from message");
            }

            var typeName = jsonObject["$type"].GetString();
            if (typeName == null)
            {
                throw new InvalidDataException("$type annotation missing from message");
            }

            var type = Type.GetType(typeName);
            return (IMessage?)JsonSerializer.Deserialize(json, type);
        }

    }
}
