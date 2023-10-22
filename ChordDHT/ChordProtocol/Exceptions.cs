using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ChordDHT.ChordProtocol.Exceptions
{
    public class RedirectException : Exception
    {
        public string TargetNode;
        public RedirectException(string targetNode) : base($"Redirect to {targetNode} required")
        {
            TargetNode = targetNode;
        }
    }

    public class RequestFailedException : Exception
    {
        public RequestFailedException(string reason) : base(reason)
        {
        }
    }

    public class  NodeGoneException : Exception
    {
        public string NodeName;

        public NodeGoneException(string nodeName) : base($"Node {nodeName} is gone")
        {
            NodeName = nodeName;
        }
    }
}
