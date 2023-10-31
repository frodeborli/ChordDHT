using ChordProtocol;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace ChordDHT.ChordProtocol
{
    public class Request<TResponse> : IRequest<TResponse>
        where TResponse : IResponse
    {
        public Guid Id { get; set; } = Guid.NewGuid();
        public Node? Sender { get; set; }
        public Node? Receiver { get; set; }
        public TResponse Filter(TResponse response)
        {
            return response;
        }
    }
}
