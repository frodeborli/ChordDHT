using Fubber;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading.Tasks;

namespace ChordProtocol
{
    public sealed class Message : EventArgs
    {
        [JsonPropertyName("source")]
        public Node Source { get; private set; }

        [JsonPropertyName("name")]
        public string Name { get; private set; }

        [JsonPropertyName("values")]
        public IDictionary<string, object?> Values { get; private set; }

        [JsonPropertyName("id")]
        public Guid Id { get; private set; }

        [JsonConstructor]
        public Message(Node source, string name, IDictionary<string, object?>? values = default, Guid? id = default)
        {
            Source = source;
            Id = id ?? Guid.NewGuid();
            Name = name;
            Values = new Dictionary<string, object?>();
            if (values != null)
            {
                foreach (var kvp in values)
                {
                    Values[kvp.Key] = kvp.Value;
                }
            }
        }

        public Message Response(Node responseNode, IDictionary<string, object?>? values = default)
        {
            return new Message(responseNode, Name, values, Id);
        }

        public object? this[string key]
        {
            get
            {
                return Values[key] ?? null;
            }
            set
            {
                Values[key] = value;
            }
        }

        public Message Clone(Node? source=null, string? name = null, IEnumerable<KeyValuePair<string, object>>? values = null)
        {
            var data = new Dictionary<string, object?>(Values);
            if (values != null)
            {
                foreach (var (key, value) in values)
                {
                    data[key] = value;
                }
            }
            return new Message(source ?? Source, name ?? Name, data);
        }

        private static object? FilterValue(object? value)
        {
            if (value is string || value is int || value is float || value is double || value is bool)
            {
                return value;
            }
            if (value is IEnumerable<KeyValuePair<string, object>> enumValues)
            {
                var dict = new Dictionary<string, object?>();
                foreach (var (key, val) in enumValues)
                {
                    dict.Add(key, FilterValue(val));
                }
                return dict;
            }
            if (value is IList<object?> listValues)
            {
                var list = new List<object?>();
                foreach (var val in listValues)
                {
                    list.Add(FilterValue(val));
                }
                return list;
            }
            return null;
        }
    }
}
