using ChordProtocol;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json.Serialization;
using System.Threading.Tasks;

namespace ChordDHT.ChordProtocol
{
    public class FingerTableEntry
    {
        [JsonPropertyName("start")]
        public ulong Start { get; set; }

        [JsonPropertyName("node")]
        public Node Node { get; set; }

        [JsonConstructor]
        public FingerTableEntry(ulong start, Node node)
        {
            Start = start;
            Node = node;
        }
    }
}
