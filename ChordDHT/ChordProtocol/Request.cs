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
        public Guid? Id { get; set; }
        public Node? Sender { get; set; }
        public Node? Receiver { get; set; }

        public Request(Guid? id = null, Node? sender = null, Node? receiver = null)
        {
            Id = id ?? Guid.NewGuid();
            Sender = sender;
            Receiver = receiver;
        }

        public TResponse Filter(TResponse response)
        {
            return response;
        }
    }
}
