using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace ChordProtocol
{
    public sealed class Message : EventArgs
    {
        public Node Source { get; private set; }
        public Guid Id { get; private set; }
        public string Name { get; private set; }
        private IDictionary<string, object?> _Data;

        private Message()
        { }

        public Message(Node source, string name, (string, object?)[] values)
        {
            Source = source;
            Id = Guid.NewGuid();
            Name = name;
            _Data = new Dictionary<string, object?>();
            foreach (var (key, value) in values)
            {
                _Data[key] = FilterValue(value);
            }
        }

        public Message(Node source, string name, IEnumerable<KeyValuePair<string, object?>> values)
            : this(source, name, values.Select(kv => (kv.Key, kv.Value)).ToArray()) { }

        public object? this[string key]
        {
            get
            {
                return _Data[key] ?? null;
            }
        }

        public Message Clone(Node? source=null, string? name = null, IEnumerable<KeyValuePair<string, object>>? values = null)
        {
            var data = new Dictionary<string, object?>(_Data);
            if (values != null)
            {
                foreach (var (key, value) in values)
                {
                    data[key] = value;
                }
            }
            return new Message(source ?? Source, name ?? Name, data);
        }

        public string Serialize()
        {
            return JsonSerializer.Serialize(this);
        }

        public static Message Deserialize(string serializedMessage)
        {
            var message = JsonSerializer.Deserialize<Message>(serializedMessage);
            if (message == null)
            {
                throw new NotSupportedException("Deserialization failed");
            }
            return message;
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
