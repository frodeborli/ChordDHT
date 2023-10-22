using ChordDHT.ChordProtocol;
using ChordDHT.ChordProtocol.Exceptions;
using ChordDHT.ChordProtocol.Messages;
using Fubber;
using System.Diagnostics;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;

namespace ChordProtocol
{
    /**
     * An implementation of the Chord protocol which is communication agnostic.
     */
    public class Chord
    {
        public Node Node { get; private set; }

        // The finger table for routing
        public Node[] Fingers { get; private set; }

        // Public read only access to the list of known nodes
        public Node[] KnownNodes {
            get
            {
                return _KnownNodes.Values.ToArray();
            }
        }

        public Node SuccessorNode { get; private set; }
        public Node PredecessorNode { get; private set; }

        public bool IsNetworked { get; private set; } = false;

        public Func<byte[], ulong> HashFunction { get; private set; }

        // Using ulong hashes which is 64 bits, therefore the finger table is constant 64 bit
        public const int FingerCount = 64;

        // Maintains a red black tree of nodes sorted according to their hash value
        private NodeList _KnownNodes;

        // Network adapter is used for inter node communications and must be provided
        private IChordNetworkAdapter NetworkAdapter;

        private Dev.LoggerContext Logger;

        public Chord(string nodeName, IChordNetworkAdapter networkAdapter, Func<byte[], ulong>? hashFunction = null)
        {
            if (nodeName == null)
            {
                throw new NullReferenceException(nameof(nodeName));
            }
            Logger = Dev.Logger("Chord");
            HashFunction = hashFunction ?? DefaultHashFunction;
            Node = new Node(nodeName, Hash(nodeName));
            NetworkAdapter = networkAdapter;

            // Just for testing
            NetworkAdapter.AddMessageHandler<Ping, Ping.Response>(async (Ping ping) => new Ping.Response());
            // When a node requests to join, we return a list of all known nodes or redirect them
            NetworkAdapter.AddMessageHandler<RequestJoin, RequestJoinResponse>(OnRequestJoin);
            // When a node says hello, we add them to our list of known nodes
            NetworkAdapter.AddMessageHandler<Hello, Acknowledge>(OnHello);
            // When a node is leaving, we'll remove them from our list of known nodes and forward the message
            NetworkAdapter.AddMessageHandler<NotifyLeaving, Acknowledge>(OnNotifyLeaving);
            // When we need to find the predecessor
            NetworkAdapter.AddMessageHandler<FindPredecessor, NodeResponse>(OnFindPredecessor);
            // When we need to find the successor
            NetworkAdapter.AddMessageHandler<FindSuccessor, NodeResponse>(OnFindSuccessor);

            // When there is only a single node, the predecessor and successor is always the same node
            PredecessorNode = Node;
            SuccessorNode = Node;

            Fingers = new Node[FingerCount];
            _KnownNodes = new NodeList(HashFunction) { Node };
            UpdateFingersTable();
            RunStabilization();
        }

        private async Task RunStabilization()
        {
            while(true)
            {
                await Task.Delay(5000);
                if (!IsNetworked)
                {
                    continue;
                }

                // Check that predecessor is alive. This also notifies him about our existence, so it will
                // fix their successor reference.
                try
                {
                    await SendMessageAsync<Acknowledge>(PredecessorNode, new Hello());
                } catch (NodeGoneException ex)
                {
                    // Predecessor seems to be gone, we will query our first successor for who may be our predecessor
                    var predecessor = await SendMessageAsync<NodeResponse>(SuccessorNode, new FindPredecessor() { hash = Node.Hash });
                    AddNode(predecessor.value);
                }

                // Check that the successor is alive. This also notifies him about our existence, so it will
                // fix their predecessor reference.
                try
                {
                    await SendMessageAsync<Acknowledge>(SuccessorNode, new Hello());
                } catch (NodeGoneException ex)
                {
                    // Predecessor seems to be gone, we will query our first successor for who may be our predecessor
                    var successor = await SendMessageAsync<NodeResponse>(SuccessorNode, new FindSuccessor() { hash = Node.Hash });
                    AddNode(successor.value);
                }
            }
        }

        public ulong Hash(byte[] key) => HashFunction(key);
        public ulong Hash(string key) => Hash(Encoding.UTF8.GetBytes(key));

        // (Ping ping) => Task.FromResult(new Ping.Response())
        private async Task<Ping.Response> OnPing(Ping ping)
        {
            await Task.Delay(1000);
            return new Ping.Response();
        }

        public async Task<Ping.Response> Ping(Node node)
        {
            return await SendMessageAsync<Ping.Response>(node, new Ping());
        }

        private Task<TResponseMessage> SendMessageAsync<TResponseMessage>(Node to, IMessage message)
            where TResponseMessage : IMessage
        {
            message.Sender = Node;
            message.Receiver = to;
            try
            {
                return NetworkAdapter.SendMessageAsync<IMessage, TResponseMessage>(message);
            } catch (NodeGoneException nodeGone)
            {
                RemoveNode(nodeGone.NodeName);
                throw nodeGone;
            }
        }

        public async Task JoinNetwork(Node nodeToJoin)
        {
            if (IsNetworked)
            {
                throw new InvalidOperationException("Already part of a network");
            }

            IsNetworked = true;

            int delay = 1000;
            RequestJoinResponse? joinResponse = null;
            do
            {
                try
                {
                    joinResponse = await SendMessageAsync<RequestJoinResponse>(nodeToJoin, new RequestJoin());
                }
                catch (Exception ex)
                {
                    Logger.Debug($"We cannot join, so we will try again in {delay} ms. Got exception:\n{ex}");
                    await Task.Delay(delay);
                    delay *= 2;
                }
            } while (joinResponse == null);

            AddNode(joinResponse.Predecessor);
            AddNode(nodeToJoin);
            AddNode(joinResponse.OtherNodes);

            /**
             * Notifying everybody ensures that we will become the successor of
             * our predecessor, and our successor will set us as his predecessor
             */
            List<Task<Acknowledge>> receivers = new List<Task<Acknowledge>>();
            foreach (var node in KnownNodes)
            {
                receivers.Add(SendMessageAsync<Acknowledge>(node, new Hello()));
            }
            await Task.WhenAll(receivers.ToArray());

            Logger.Info("Successfully joined chord network");
        }

        private Task<RequestJoinResponse> OnRequestJoin(RequestJoin request)
        {
            var nodeToJoinAt = Lookup(request.Sender!.Name);

            if (nodeToJoinAt != Node)
            {
                throw new RedirectException(nodeToJoinAt.ToString());
            }

            return Task.FromResult(new RequestJoinResponse() { Predecessor = PredecessorNode, OtherNodes = KnownNodes.ToArray() });
        }

        private Task<Acknowledge> OnHello(Hello request)
        {
            AddNode(request.Sender);
            return Task.FromResult(new Acknowledge());
        }

        private async Task<Acknowledge> OnNotifyLeaving(NotifyLeaving request)
        {
            if (request.ForwardedFor != null)
            {
                RemoveNode(request.ForwardedFor);
            } else {
                RemoveNode(request.Sender);

                // Tell everybody we know that we received a notification
                List<Task<Acknowledge>> receivers = new List<Task<Acknowledge>>();
                foreach (var node in KnownNodes)
                {
                    receivers.Add(SendMessageAsync<Acknowledge>(node, new NotifyLeaving() { ForwardedFor = request.Sender }));
                }
                await Task.WhenAll(receivers.ToArray());
            }
            return new Acknowledge();
        }

        private Task<NodeResponse> OnFindPredecessor(FindPredecessor request)
        {
            var node = Lookup(request.hash);
            if (node == SuccessorNode)
            {
                // The key appears to belong on our successor according to our finger
                // table, so our successor is the predecessor
                return Task.FromResult(new NodeResponse() { value = SuccessorNode });
            } else if (node == Node)
            {
                // The key seems to belong here, but we will let our predecessor
                // make the final call
                throw new RedirectException(PredecessorNode.Name);
            } else 
            {
                // The hash does not belong with us
                throw new RedirectException(node.Name);
            }
        }

        private Task<NodeResponse> OnFindSuccessor(FindSuccessor request)
        {
            var node = Lookup(request.hash);

            if (node == Node)
            {
                // If the lookup returns our own node, then we are the successor.
                return Task.FromResult(new NodeResponse() { value = Node });
            }
            else
            {
                // Otherwise, we redirect the requester to the node as suggested by our finger table.
                throw new RedirectException(node.Name);
            }
        }

        public async Task LeaveNetwork()
        {
            if (!IsNetworked)
            {
                throw new InvalidOperationException("We are not networked, so we can't leave.");
            }
            IsNetworked = false;

            // Tell everybody we know that we are leaving
            List<Task<Acknowledge>> receivers = new List<Task<Acknowledge>>();
            foreach (var node in KnownNodes)
            {
                receivers.Add(SendMessageAsync<Acknowledge>(node, new NotifyLeaving()));
            }
            await Task.WhenAll(receivers.ToArray());
        }

        public void AddNode(IEnumerable<Node> nodes)
        {
            int addedCount = 0;
            lock (_KnownNodes)
            {
                foreach (var node in nodes)
                {
                    if (!_KnownNodes.ContainsKey(node.Name))
                    {
                        _KnownNodes.Add(node);
                        Logger.Debug(" - added node {node} to list of known nodes");
                        addedCount++;
                    }
                }
            }
            if (addedCount > 0)
            {
                UpdateFingersTable();
            }
        }

        public void AddNode(Node node) => AddNode(new Node[] { node });

        /**
         * Add knowledge about the existence of a node. This node
         * might not be used in routing, depending on the hash value
         * of the node name.
         */
        public void AddNode(IEnumerable<string> nodes)
        {
            int addedCount = 0;
            lock (_KnownNodes)
            {
                foreach (var node in nodes)
                {
                    if (node == null)
                    {
                        throw new ArgumentNullException(nameof(nodes), "Found a null string in AddNode");
                    }
                    if (!_KnownNodes.ContainsKey(node))
                    {
                        _KnownNodes.Add(node);
                        Dev.Debug($" - {Node.Name}: Added node '{node}' to list of known nodes");
                        addedCount++;
                    }
                }
            }
            if (addedCount > 0)
            {
                UpdateFingersTable();
            }
        }

        public void AddNode(string nodeName) => AddNode(new string[] { nodeName });

        public void RemoveNode(IEnumerable<Node> nodes) => RemoveNode(nodes.Select(n => n.Name));
        public void RemoveNode(Node node) => RemoveNode(node.Name);

        /**
         * Remove knowledge about the existence of a node. This 
         * node might not be used in routing, depending on the hash
         * value of the node name - but removing it ensures that the
         * node will not end up in the finger table in the future.
         */
        public void RemoveNode(IEnumerable<string> nodes)
        {
            int removedCount = 0;
            lock (_KnownNodes)
            {
                foreach (var node in nodes)
                {
                    if (node == null)
                    {
                        throw new ArgumentNullException(nameof(nodes), "Found a null string in RemoveNode");
                    }
                    if (node == Node.Name)
                    {
                        throw new InvalidOperationException($"Illegal to remove self from known nodes list");
                    }
                    if (_KnownNodes.Remove(node))
                    {
                        Dev.Debug($" - {Node.Name}: Removed node '{node}' from list of known nodes");
                        removedCount++;
                    }
                }
            }
            if (removedCount > 0)
            {
                UpdateFingersTable();
            }
        }

        public void RemoveNode(string nodeName) => RemoveNode(new string[] { nodeName });

        public void TagNode(string nodeName)
        {
            _KnownNodes[nodeName].Tag();
        }

        /**
         * Given a key, find which node is the best node to answer the question
         */
        public Node Lookup(ulong keyHash)
        {
            ulong distance;

            // Check if this key belongs to me
            if (Inside(keyHash, PredecessorNode.Hash + 1, Node.Hash))
            {
                return Node;
            }

            // Handle wrapping of node ids
            if (keyHash > Node.Hash)
            {
                distance = keyHash - Node.Hash;
            }
            else
            {
                distance = (ulong.MaxValue - Node.Hash) + keyHash + 1;
            }

            int fingerIndex = (int)Math.Floor(Math.Log2(distance));

            return Fingers[Math.Min(fingerIndex, Fingers.Length - 1)];
        }

        public Node Lookup(byte[] key) => Lookup(Hash(key));

        public Node Lookup(string key) => Lookup(Encoding.UTF8.GetBytes(key));

        /**
         * Request an update of the fingers table. The fingers table
         * will be updated as soon as the thread pool is available.
         */
        private void UpdateFingersTable()
        {
            // Avoid problems with concurrent processes causing the fingers table to be updated
            lock (Fingers)
            {
                for (int i = 0; i < FingerCount; i++)
                {
                    var node = _KnownNodes.FindSuccessor(Node.Hash + (1UL << i));
                    if (node == null)
                    {
                        throw new InvalidOperationException("Can't generate fingers table without having knowledge about any nodes");
                    }
                    Fingers[i] = node;
                }
            }
        }

        /**
         * Check if hash is between floor and ceiling inclusive, while
         * allowing wrapping around the key space size.
         */
        private bool Inside(ulong hash, ulong floor, ulong ceiling)
        {
            if (floor <= ceiling)
            {
                return hash >= floor && hash <= ceiling;
            }
            else
            {
                return hash <= ceiling || hash >= floor;
            }
        }

        /**
         * The default hash function if no custom hash function is provided
         */
        public static ulong DefaultHashFunction(byte[] key)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key), "Key cannot be null");
            }
            using (SHA1 sha1 = SHA1.Create())
            {
                byte[] hash = sha1.ComputeHash(key);

                // Convert the hash to an UInt64 (not using BitConverter.ToUInt64 because we want to maintain endianness accross architectures)
                return BitConverter.ToUInt64(hash, 0);
            }
        }

    }
}
