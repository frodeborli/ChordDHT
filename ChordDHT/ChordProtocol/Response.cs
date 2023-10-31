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
    public abstract class Response : IResponse
    {
        public Guid Id { get; set; } = Guid.NewGuid();

        public Node? Sender { get; set; }

        public Node? Receiver { get; set; }
    }
}
