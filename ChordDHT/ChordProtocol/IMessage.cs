using ChordProtocol;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace ChordDHT.ChordProtocol
{
    public interface IMessage
    {
        public Node Sender { get; set; }
        public Node Receiver { get; set; }

        public string ToJson();
    }
}
