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
    public class QueryPredecessor : Request<RoutingResponse>
    {
        public ulong Hash { get; set; }

        [JsonConstructor]
        public QueryPredecessor(ulong hash)
        {
            Hash = hash;
        }

        public QueryPredecessor(Node node) : this(node.Hash) { }
    }

    /// <summary>
    /// Request to find the successor of the given Hash value
    /// </summary>
    public class QuerySuccessor : Request<RoutingResponse>
    {
        public ulong Hash { get; set; }

        [JsonConstructor]
        public QuerySuccessor(ulong hash)
        {
            Hash = hash;
        }

        public QuerySuccessor(Node node) : this(node.Hash) { }
    }

    /// <summary>
    /// Notification to other nodes that `LeavingNode` is leaving the network
    /// </summary>
    public class LeavingNetwork : Request<AckResponse>
    {
        public Node LeavingNode { get; set; }
        public Node SuccessorNode { get; set;  }
        public Node PredecessorNode { get; set; }

        [JsonConstructor]
        public LeavingNetwork(Node leavingNode, Node successorNode, Node predecessorNode)
        {
            LeavingNode = leavingNode;
            SuccessorNode = successorNode;
            PredecessorNode = predecessorNode;
        }
    }

    public class RequestNodeInfo : Request<NodeInfoResponse> { }

    /// <summary>
    /// Request to get information about another node
    /// </summary>
    public class  NodeInfoResponse : Response
    {
        public Node? PredecessorNode { get; set; }
        public Node SuccessorNode { get; set; }
        public ulong Hash { get; set; }
        public string Name { get; set; }

        public List<Node> Successors { get; set; }

        [JsonConstructor]
        public NodeInfoResponse(Node? predecessorNode, Node successorNode, List<Node> successors, ulong hash, string name)
        {
            PredecessorNode = predecessorNode;
            SuccessorNode = successorNode;
            Successors = successors;
            Hash = hash;
            Name = name;
        }
    }

    /// <summary>
    /// Request to join the network at the receiver node
    /// </summary>
    public class JoinNetwork : Request<JoinNetworkResponse> {
    }

    /// <summary>
    /// Generic response for accepting a network join request.
    /// </summary>
    public class JoinNetworkResponse : Response
    {
        /// <summary>
        /// The successor for the joining node
        /// </summary>
        public Node SuccessorNode { get; set; }

        /// <summary>
        /// The predecessor for the joining node
        /// </summary>
        public Node PredecessorNode { get; set; }

        public List<Node> Successors { get; set; }

        [JsonConstructor]
        public JoinNetworkResponse(Node successorNode, Node predecessorNode, List<Node> successors)
        {
            SuccessorNode = successorNode;
            PredecessorNode = predecessorNode;
            Successors = successors;
        }
    }

    public class RequestAsSuccessor : Request<MakeSuccessorResponse>
    {
        public Node FailedSuccessor { get; set; }

        [JsonConstructor]
        public RequestAsSuccessor(Node failedSuccessor)
        {
            FailedSuccessor = failedSuccessor;
        }
    }

    public class MakeSuccessorResponse : Response
    {
        public List<Node> Successors { get; set; }

        public MakeSuccessorResponse(List<Node> successors)
        {
            Successors = successors;
        }
    }

    /// <summary>
    /// Notify a node that a new successor is being inserted. This message comes from
    /// the current successor as part of the join process.
    /// </summary>
    public class NotifyNewSuccessor : Request<AckResponse>
    {
        /// <summary>
        /// The node that is joining and should become the new successor of
        /// the predecessor node of SuccessorNode.
        /// </summary>
        public Node JoiningNode { get; set; }

        /// <summary>
        /// The node that is handling the join and accepting JoiningNode into
        /// the network.
        /// </summary>
        public Node SuccessorNode { get; set; }

        public List<Node> Successors { get; set; }

        [JsonConstructor]
        public NotifyNewSuccessor(Node joiningNode, Node successorNode, List<Node> successors)
        {
            JoiningNode = joiningNode;
            SuccessorNode = successorNode;
            Successors = successors;
        }
    }

    /// <summary>
    /// Generic response message which contains no other data.
    /// </summary>
    public class AckResponse : Response { }

    /// <summary>
    /// Generic response for finding a node by traversing the Chord ring.
    /// </summary>
    public class RoutingResponse : Response
    {
        public Node Node { get; set; }
        public bool IsRedirect { get; set; }

        [JsonConstructor]
        public RoutingResponse(Node node, bool isRedirect)
        {
            Node = node;
            IsRedirect = isRedirect;
        }
    }

    public class BoolResponse : Response
    {
        public bool Value { get; set; }

        [JsonConstructor]
        public BoolResponse(bool value)
        {
            Value = value;
        }
    }
}
