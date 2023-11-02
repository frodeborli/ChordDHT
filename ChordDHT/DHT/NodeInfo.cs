using ChordDHT.ChordProtocol;
using ChordProtocol;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json.Serialization;
using System.Threading.Tasks;

namespace ChordDHT.DHT
{
    internal class NodeInfo
    {
        [JsonPropertyName("node_hash")]
        public string NodeHash { get; set; }

        [JsonPropertyName("node_name")]
        public string NodeName { get; set; }

        [JsonPropertyName("others")]
        public string[] KnownNodes { get; set; }

        [JsonPropertyName("predecessor")]
        public string Predecessor { get; set; }

        [JsonPropertyName("successor")]
        public string Successor { get; set; }

        [JsonPropertyName("stable_time")]
        public TimeSpan TimeSinceLastFingerTableUpdate { get; set;  }

        [JsonPropertyName("finger_table")]
        public FingerTableEntry[] FingerTable { get; set; }
    }
}
