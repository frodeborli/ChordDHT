﻿using ChordDHT.ChordProtocol;
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
        [JsonConverter(typeof(Util.JsonUlongAsHex))]
        public ulong Hash { get; private set; }

        [JsonIgnore]
        public DateTime LastSeen
        {
            get; private set;
        }

        [JsonIgnore]
        public int? HopCount { get; } = null;

        [JsonConstructor]
        public Node(string name, ulong hash)
        {
            Name = name;
            Hash = hash;
            Tag();
        }

        public Node(Node node, int hopCount)
        {
            Name = node.Name;
            Hash = node.Hash;
            HopCount = hopCount;
        }

        public void Tag()
        {
            LastSeen = DateTime.UtcNow;
        }

        override public string ToString()
        {
            return $"[Node {Name} {Util.Percent(Hash)}% ({Hash})]";
        }

        public override int GetHashCode()
        {
            return (int)Hash;
        }

        public override bool Equals(object? obj)
        {
            return obj is Node node && Name == node.Name;
        }
        public static bool operator ==(Node? a, Node? b)
        {
            if (ReferenceEquals(a, null) && ReferenceEquals(b, null)) return true;
            if (ReferenceEquals(a, null) || ReferenceEquals(b, null)) return false;
            return a.Name == b.Name;
        }

        public static bool operator !=(Node? a, Node? b)
        {
            return !(a == b);
        }

    }
}
