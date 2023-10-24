using ChordProtocol;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ChordDHT.ChordProtocol.Messages
{

    public class FindPredecessor : Message
    {
        public ulong hash { get; set; }
    }

    public class FindSuccessor : Message
    {
        public ulong hash { get; set; }
    }

    public class NotifyLeaving : Message {
        public Node? ForwardedFor { get; set; } = null;
    }

    public class GetNodeInfo : Message { }

    public class  GetNodeInfoResponse : Message
    {
        public Node PredecessorNode { get; set; }
        public Node SuccessorNode { get; set; }
    }

    public class RequestJoin : Message { }

    public class RequestJoinResponse : Message
    {
        public Node Predecessor { get; set; }
        public Node[] OtherNodes { get; set; }
    }

    /// <summary>
    /// Notify a node about our existence
    /// </summary>
    public class Hello : Message { }

    /// <summary>
    /// Generic message class to acknowledge receiving the message
    /// </summary>
    public class Acknowledge : Message { }

    public class StringResponse : Message
    {
        public string value { get; set; }
    }

    public class NodeResponse : Message
    {
        public Node value { get; set; }
    }

    public class Ping : Message
    {
        public string Message { get; set; } = "ping";

        public class Response : Message {}

    }
}
