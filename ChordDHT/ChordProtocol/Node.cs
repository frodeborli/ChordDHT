using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json.Serialization;
using System.Threading.Tasks;

namespace ChordProtocol
{
    public class Node
    {
        [JsonPropertyName("name")]
        public string Name { get; private set; }

        [JsonPropertyName("hash")]
        public ulong Hash { get; private set; }

        [JsonIgnore]
        public DateTime LastSeen
        {
            get; private set;
        }

        public Node(string name, ulong hash)
        {
            Name = name;
            Hash = hash;
            Tag();
        }

        public void Tag()
        {
            LastSeen = DateTime.UtcNow;
        }

        public string ToString()
        {
            return $"[Node {Name}]";
        }

        public static bool operator ==(Node? a, Node? b)
        {
            if (ReferenceEquals(a, null) && ReferenceEquals(b, null)) return true;
            if (ReferenceEquals(a, null) || ReferenceEquals(b, null)) return false;
            return a.Name == b.Name;
        }

        public static bool operator !=(Node? a, Node? b)
        {
            return a.Name != b.Name;
        }

    }
}
