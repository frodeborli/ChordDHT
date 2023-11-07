using ChordProtocol;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ChordDHT.ChordProtocol.Exceptions
{

    public class MaxHopCountReached : Exception
    { }

    /// <summary>
    /// When a request will not be fulfilled
    /// </summary>
    public class RejectedException : Exception
    {
        public RejectedException(string reason) : base(reason)
        {
        }
    }

    /// <summary>
    /// When a request fails due to any other problem (network or protocol errors)
    /// </summary>
    public class NetworkException : Exception
    {
        public Node Node;

        public NetworkException(Node node, string? reason=null) : base($"Node {node.Name} is gone{(reason != null ? $": {reason}" : "")}")
        {
            Node = node;
        }
    }
}
