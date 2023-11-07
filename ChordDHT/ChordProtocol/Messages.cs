using ChordProtocol;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json.Serialization;
using System.Threading.Tasks;

namespace ChordDHT.ChordProtocol.Messages
{
    /// <summary>
    /// Request to find the predecessor of the given Hash value
    /// </summary>
    public class FindPredecessor : Request<FindPredecessorReply>
    {
        public ulong Hash { get; set; }

        [JsonConstructor]
        public FindPredecessor(ulong hash, Node? sender = null) : base(sender)
        {
            Hash = hash;
        }

        public override string ToString()
        {
            return $"(FindPredecessor hash={Hash} ({Util.Percent(Hash)}%) from={(Sender != null ? Sender : "NULL")})";
        }
    }

    /// <summary>
    /// Response for the FindPredecessor query
    /// </summary>
    public class FindPredecessorReply : Response
    {
        public Node Node { get; set; }

        public bool IsRedirect { get; set; }

        [JsonConstructor]
        public FindPredecessorReply(Node node, bool isRedirect)
        {
            Node = node;
            IsRedirect = isRedirect;
        }

        public override string ToString()
        {
            return $"(FindPredecessor-reply result={Node})";
        }
    }

    public class InformNodeGone : Request<AcknowledgeReply>
    {
        public Node GoneNode { get; set; }

        [JsonConstructor]
        public InformNodeGone(Node goneNode, Node? sender = null) : base(sender)
        {
            GoneNode = goneNode;
        }
    }

    /// <summary>
    /// Request to find the predecessor of the given Hash value
    /// </summary>
    public class FindSuccessor : Request<FindSuccessorReply>
    {
        public ulong Hash { get; set; }

        [JsonConstructor]
        public FindSuccessor(ulong hash, Node? sender = null) : base(sender)
        {
            Hash = hash;
        }

        public override string ToString()
        {
            return $"(FindSuccessor hash={Hash} ({Util.Percent(Hash)}%) from={(Sender != null ? Sender : "NULL")})";
        }
    }

    /// <summary>
    /// Response for the FindPredecessor query
    /// </summary>
    public class FindSuccessorReply : Response
    {
        public Node Node { get; set; }
        public bool IsRedirect { get; set; }

        [JsonConstructor]
        public FindSuccessorReply(Node node, bool isRedirect)
        {
            Node = node;
            IsRedirect = isRedirect;
        }

        public override string ToString()
        {
            return $"(FindSuccessor-reply result={Node})";
        }
    }

    /// <summary>
    /// Notification to other nodes that `LeavingNode` is leaving the network
    /// </summary>
    public class LeavingNetwork : Request<AcknowledgeReply>
    {
        public Node LeavingNode { get; set; }
        public Node SuccessorNode { get; set;  }
        public Node PredecessorNode { get; set; }

        [JsonConstructor]
        public LeavingNetwork(Node leavingNode, Node successorNode, Node predecessorNode, Node? sender = null) : base(sender)
        {
            LeavingNode = leavingNode;
            SuccessorNode = successorNode;
            PredecessorNode = predecessorNode;
        }

        public override string ToString()
        {
            return $"(LeavingNetwork leaving={LeavingNode} their-predecessor={PredecessorNode} their-successor={SuccessorNode} from={(Sender != null ? Sender : "NULL")})";
        }
    }

    public class RequestNodeInfo : Request<NodeInfoReply> {
        [JsonConstructor]
        public RequestNodeInfo(Node? sender = null) : base(sender)
        { }
        public override string ToString()
        {
            return $"(RequestNodeInfo from={(Sender != null ? Sender : "NULL")})";
        }
    }

    /// <summary>
    /// Request to get information about another node
    /// </summary>
    public class NodeInfoReply : Response
    {
        public Node? PredecessorNode { get; set; }
        public Node SuccessorNode { get; set; }
        public ulong Hash { get; set; }
        public string Name { get; set; }

        public List<Node> Successors { get; set; } = new List<Node>();

        [JsonConstructor]
        public NodeInfoReply(Node? predecessorNode, Node successorNode, List<Node> successors, ulong hash, string name)
        {
            PredecessorNode = predecessorNode;
            SuccessorNode = successorNode;
            Successors = successors;
            Hash = hash;
            Name = name;
        }
        public override string ToString()
        {
            return $"(NodeInfo-reply my-predecessor={PredecessorNode} my-successor={SuccessorNode} my-successors=[{string.Join(",", Successors)}])";
        }

    }

    /// <summary>
    /// Request to join the network at the receiver node
    /// </summary>
    public class JoinNetwork : Request<JoinNetworkReply> {

        [JsonConstructor]
        public JoinNetwork(Node? sender = null) : base(sender)
        { }
        public override string ToString()
        {
            return $"(JoinNetwork from={(Sender != null ? Sender : "NULL")})";
        }
    }

    /// <summary>
    /// Generic response for accepting a network join request.
    /// </summary>
    public class JoinNetworkReply : Response
    {
        /// <summary>
        /// The successor for the joining node
        /// </summary>
        public Node SuccessorNode { get; set; }

        /// <summary>
        /// The predecessor for the joining node
        /// </summary>
        public Node PredecessorNode { get; set; }

        public List<Node> Successors { get; set; } = new List<Node>();

        [JsonConstructor]
        public JoinNetworkReply(Node predecessorNode, Node successorNode, List<Node> successors)
        {
            SuccessorNode = successorNode;
            PredecessorNode = predecessorNode;
            Successors = successors;
        }

        public override string ToString()
        {
            return $"(JoinNetwork-reply your-predecessor={PredecessorNode} your-successor={SuccessorNode} your-successors=[{string.Join(",", Successors)}])";
        }

    }

    public class BecomeMySuccessor : Request<BecomeMySuccessorReply>
    {
        public Node FailedSuccessor { get; set; }

        [JsonConstructor]
        public BecomeMySuccessor(Node failedSuccessor, Node? sender = null) : base(sender)
        {
            FailedSuccessor = failedSuccessor;
        }

        public override string ToString()
        {
            return $"(BecomeMySuccessor my-failed-successor={FailedSuccessor} from={(Sender != null ? Sender : "NULL")})";
        }
    }

    public class BecomeMySuccessorReply : Response
    {
        public List<Node>? Successors { get; set; } = null;
        public Node? BetterPredecessor { get; set; } = null;

        public BecomeMySuccessorReply(List<Node>? successors)
        {
            Successors = successors;
            BetterPredecessor = null;
        }

        public BecomeMySuccessorReply(Node redirectTo)
        {
            Successors = null;
            BetterPredecessor = redirectTo;
        }

        [JsonConstructor]
        public BecomeMySuccessorReply(List<Node>? successors, Node? betterPredecessor)
        {
            Successors = successors;
            BetterPredecessor = betterPredecessor;
        }

        public override string ToString()
        {
            return $"(BecomeMySuccessor-reply your-predecessor={BetterPredecessor} your-successors=[{string.Join(", ", Successors)}])";
        }

    }

    /// <summary>
    /// Notify a node that a new successor is being inserted. This message comes from
    /// the current successor as part of the join process.
    /// </summary>
    public class YouHaveNewSuccessor : Request<AcknowledgeReply>
    {
        /// <summary>
        /// The node that is joining and should become the new successor of
        /// the predecessor node of SuccessorNode.
        /// </summary>
        public Node SuccessorNode { get; set; }

        public List<Node> Successors { get; set; } = new List<Node>();

        [JsonConstructor]
        public YouHaveNewSuccessor(Node successorNode, List<Node> successors, Node? sender = null) : base(sender)
        {
            SuccessorNode = successorNode;
            Successors = successors;
        }

        public override string ToString()
        {
            return $"(YouHaveNewSuccessor your-successor={SuccessorNode} your-successors=[{string.Join(", ", Successors)}] from={(Sender != null ? Sender : "NULL")})";
        }
    }

    /// <summary>
    /// Generic response message which contains no other data.
    /// </summary>
    public class AcknowledgeReply : Response {

        private static AcknowledgeReply? _instance = null;
        public static AcknowledgeReply Instance
        {
            get
            {
                if (_instance == null)
                {
                    _instance = new AcknowledgeReply();
                }
                return _instance;
            }
        }           

        public override string ToString()
        {
            return $"(Acknowledge-reply)";
        }

    }


}
