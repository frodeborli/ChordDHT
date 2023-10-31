using ChordDHT.ChordProtocol;
using ChordDHT.ChordProtocol.Exceptions;
using ChordDHT.ChordProtocol.Messages;
using ChordDHT.Fubber;
using Fubber;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;

namespace ChordProtocol
{
    /// <summary>
    /// Implementation of the Chord Protocol with a pluggable networking backend
    /// </summary>
    public class Chord
    {
        /// <summary>
        /// The node represented by this instance of this class.
        /// </summary>
        public Node Node { get; private set; }

        /// <summary>
        /// Represents each of the fingers of the finger table. The order of the
        /// fingers is according to the Chord Protocol.
        /// </summary>
        public Node[] Finger { get; private set; }

        /// <summary>
        /// The identity of the immediate successor node. This should be identical
        /// to the node in the first finger in the finger table. For a chord network
        /// having only a single node, the SuccessorNode is identical to this.Node.
        /// </summary>
        public Node SuccessorNode { get; private set; }

        /// <summary>
        /// The identity of the immediate predecessor node. For a single node Chord
        /// network, the PredecessorNode is identical to this.Node. In a multi node
        /// chord network, the PredecessorNode can be in a failed state.
        /// </summary>
        public Node PredecessorNode { get; private set; }

        /// <summary>
        /// If the predecessor is failed, we must wait for another node to propose that
        /// we are its successor. Meanwhile we have to accept having a failed predecessor.
        /// </summary>
        public bool PredecessorFailed { get; private set; } = false;

        /// <summary>
        /// If our successor is in a failed state, we can't route requests to it and this
        /// is a transient state while the stabilization finds a new successor.
        /// </summary>
        public bool SuccessorFailed { get; private set; } = false;

        /// <summary>
        /// Allows for dependency injection of the Hash function. The function must always
        /// return a 64 bit hash value and the same hash function must be used on all participating
        /// nodes.
        /// </summary>
        public Func<byte[], ulong> HashFunction { get; private set; }

        /// <summary>
        /// By definition, a Chord network using 64 bit hashes will have 64 fingers in the 
        /// finger table.
        /// </summary>
        public const int FingerCount = 64;

        /// <summary>
        /// The number of successors that a node is allowed to keep as failover if
        /// the active successor fails
        /// </summary>
        public const int MaxSuccessorCount = 10;

        /// <summary>
        /// Allows for dependency injection of the networking interface which is used to facilitate
        /// communication between nodes.
        /// </summary>
        private INetworkAdapter NetworkAdapter;

        /// <summary>
        /// Allows for dependency injection of the logging interface which is used to emit log events
        /// </summary>
        private ILogger Logger;

        /// <summary>
        /// A list of failover successors for the node. The list is retrieved from the successor upon
        /// joining the network.
        /// </summary>
        private List<Node> SuccessorsList = new List<Node>();

        /// <summary>
        /// Object used for locking the join process
        /// </summary>
        private LockManager UpdateLock = new LockManager();

        /// <summary>
        /// Caches nodes that have been recently added to the finger table, to the cost.
        /// </summary>
        private Cache<Node, Node> FingerTableAddCache = new Cache<Node, Node>(32, TimeSpan.FromSeconds(10));

        /// <summary>
        /// Caches QuerySuccessor results for 1.5 seconds.
        /// </summary>
        private Cache<ulong, Node> QuerySuccessorCache = new Cache<ulong, Node>(1024, TimeSpan.FromSeconds(5));

        /// <summary>
        /// Construct a Chord node instance.
        /// </summary>
        /// <param name="nodeName">The name of the node. This name must have a form that is recognizable by the network adapter and can be used to connect to other nodes</param>
        /// <param name="networkAdapter">The network adapter implementation</param>
        /// <param name="logger">The logging implementation</param>
        /// <param name="hashFunction">The hash function, which defaults to Chord.HashFunction</param>
        /// <exception cref="NullReferenceException"></exception>
        public Chord(string nodeName, INetworkAdapter networkAdapter, ILogger logger, Func<byte[], ulong>? hashFunction = null)
        {
            if (nodeName == null)
            {
                throw new NullReferenceException(nameof(nodeName));
            }
            Logger = logger;
            HashFunction = hashFunction ?? Util.Sha1Hash;
            Node = new Node(nodeName, Hash(nodeName));
            NetworkAdapter = networkAdapter;

            SetupMessageHandlers();

            // When there is only a single node, the predecessor and successor is always the same node
            PredecessorNode = Node;
            SuccessorNode = Node;

            // Creating an initial fingers table, where everything points to us
            Finger = new Node[FingerCount];

            for (var i = 0; i < FingerCount; i++)
            {
                Finger[i] = Node;
            }

            //_KnownNodes = new NodeList(HashFunction) { Node };
            //UpdateFingersTable();
        }

        private void SetupMessageHandlers()
        {
            NetworkAdapter.AddHandler(async (JoinNetwork request) => 
            {

                Logger.Info($"Node {request.Sender} is requesting to join the network");

                if (!await UpdateLock.AcquireAsync(TimeSpan.FromSeconds(3)))
                {
                    Logger.Debug("Unable to aquire lock for join request");
                    throw new RejectedException("Busy, try again.");
                }

                try
                {
                    if (!Util.Inside(request.Sender.Hash, PredecessorNode.Hash + 1, Node.Hash))
                    {
                        Logger.Info($"Rejecting join for {request.Sender} because it is joining on the incorrect successor ({PredecessorNode} < {request.Sender} <= {Node})");
                        throw new RejectedException("Must join at the correct successor");
                    }
                    else if (PredecessorFailed)
                    {
                        Logger.Info($"Rejecting join for {request.Sender} because of a failed predecessor");
                        throw new RejectedException("Temporarily can't accept joins");
                    }


                    var myOldPredecessor = PredecessorNode;

                    if (PredecessorNode == Node)
                    {
                        // No need to notify about the new successor
                        SuccessorNode = request.Sender;
                        SuccessorsList = new List<Node> { request.Sender };
                    }
                    else
                    {
                        try
                        {
                            var successorsList = MakeSuccessorsList();
                            successorsList.Insert(0, PredecessorNode);
                            // Notify the predecessor about the new joining node
                            await SendMessageAsync(PredecessorNode, new NotifyNewSuccessor(request.Sender, Node, successorsList));
                        }
                        catch (Exception ex)
                        {
                            Logger.Debug($"Exception when notifying predecessor {PredecessorNode} about the new joining node {request.Sender}:\n{ex}");
                            throw new RejectedException("Unable to update predecessor node with new successor:\n{ex}");
                        }
                    }

                    PredecessorNode = request.Sender;
                    
                    var newSuccessorList = MakeSuccessorsList();
                    return new JoinNetworkResponse(Node, myOldPredecessor, newSuccessorList);
                }
                finally
                {
                    UpdateLock.Release();
                }
            });

            NetworkAdapter.AddHandler(async (LeavingNetwork request) => 
            {
                Logger.Info($"Node {request.LeavingNode} is leaving");

                if (!await UpdateLock.AcquireAsync(TimeSpan.FromSeconds(3)))
                {
                    Logger.Debug("Unable to aquire lock for leave network request");
                    throw new RejectedException("Busy, try again");
                }

                try
                {
                    if (request.LeavingNode == SuccessorNode)
                    {
                        Logger.Info($"{request.LeavingNode} leaving: Replacing successor {SuccessorNode} with {request.SuccessorNode}");
                        SuccessorNode = request.SuccessorNode;
                    }

                    // If it is our predecessor that is leaving, take over its successor
                    if (request.LeavingNode == PredecessorNode)
                    {
                        Logger.Info($"{request.LeavingNode} leaving: Replacing predecessor {PredecessorNode} with {request.PredecessorNode}");
                        // To do: We should've transferred all keys from our predecessor before this completes
                        PredecessorNode = request.PredecessorNode;
                    }

                    FingerTableRemove(request.LeavingNode);
                }
                finally
                {
                    UpdateLock.Release();
                }

                return new AckResponse();
            });

            NetworkAdapter.AddHandler((QuerySuccessor request) =>
            {
                lock (Finger)
                {
                    if (PredecessorNode == Node)
                    {
                        // We aren't networked so we are always both the predecessor and successor.
                        return Task.FromResult(new RoutingResponse(Node, false));
                    }
                    else if (Util.Inside(request.Hash, Node.Hash + 1, SuccessorNode.Hash))
                    {
                        // The hash is between our hash + 1 and our successors hash, so our successor is the successor
                        return Task.FromResult(new RoutingResponse(SuccessorNode, false));
                    }
                }


                // Query the network
                var (predecessor, successor) = FingerTableQuery(request.Hash);

                var response = new RoutingResponse(predecessor, true);

                return Task.FromResult(response);
            });

            NetworkAdapter.AddHandler((RequestNodeInfo request) =>
            {
                return Task.FromResult(new NodeInfoResponse(PredecessorNode, SuccessorNode, MakeSuccessorsList(), Node.Hash, Node.Name));
            });

            NetworkAdapter.AddHandler(async (NotifyNewSuccessor request) =>
            {

                if (!await UpdateLock.AcquireAsync(TimeSpan.FromSeconds(3)))
                {
                    Logger.Debug("Unable to aquire lock for notify new successor request");
                    throw new RejectedException("Busy, try again");
                }

                try
                {
                    if (request.SuccessorNode != SuccessorNode)
                    {
                        throw new RejectedException("We will only accept join notifications from our successor");
                    }

                    SuccessorNode = request.JoiningNode;

                    // Ensure this node goes straight into the finger table
                    //FingerTableAdd(request.JoiningNode);
                }
                finally
                {
                    UpdateLock.Release();
                }

                return new AckResponse();
            });

            NetworkAdapter.AddHandler(async (RequestAsSuccessor request) =>
            {
                if (!await UpdateLock.AcquireAsync(TimeSpan.FromSeconds(3)))
                {
                    Logger.Debug("Unable to aquire lock for request as successor request");
                    throw new RejectedException("Busy, try again");
                }

                try
                {
                    if (PredecessorFailed)
                    {
                        Logger.Info($"Accepting a new predecessor {request.Sender} since our current predecessor is failed");
                        PredecessorNode = request.Sender;
                        PredecessorFailed = false;
                        return new MakeSuccessorResponse(MakeSuccessorsList());
                    }
                    else
                    {
                        Logger.Notice($"Rejecting new predecessor {request.Sender} since our current predecessor is not marked as failed");
                        throw new RejectedException("Predecessor is still alive");
                    }
                } finally
                {
                    UpdateLock.Release();
                }
            });
        }

        /// <summary>
        /// Find the nearest successor including key (searching for key 123 can return node 123)
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        public async Task<Node> QuerySuccessor(ulong key, Node? viaNode = default, bool autoRedirect = true)
        {
            if (QuerySuccessorCache.TryGetValue(key, out var cachedNode, true))
            {
                return cachedNode!;
            }

            Node? targetNode;
            if (viaNode == null)
            {

                /**
                 * If our successor is the successor, we will resolve this request
                 */
                if (Util.Inside(key, Node.Hash + 1, SuccessorNode.Hash))
                {
                    // Our successor is the successor
                    return new Node(SuccessorNode, 0);
                }

                /**
                 * Using the finger table, find the nearest predecessor
                 */
                var (predecessor, successor) = FingerTableQuery(key);
                if (SuccessorFailed && predecessor == SuccessorNode)
                {
                    if (successor == null)
                    {
                        throw new Exception("Weird situation, where we don't get a successor finger, and the finger is our successor");
                    }
                    // We can't use our successor since it is in a failed state, so we use the next finger
                    targetNode = successor;
                }
                else
                {
                    targetNode = predecessor;
                }
            }
            else
            {
                targetNode = viaNode;
            }

            int hopCount = 0;
            while (true)
            {
                try
                {
                    hopCount++;
                    var result = await SendMessageAsync(targetNode, new QuerySuccessor(key));

                    if (result.IsRedirect)
                    {
                        targetNode = result.Node;
                    }
                    else
                    {
                        return QuerySuccessorCache[key] = new Node(result.Node, hopCount);
                    }
                }
                catch
                {
                    FingerTableRemove(targetNode);
                    Logger.Notice($"Node {targetNode} did not respond. If it was in the finger table, the reference was removed.");
                    if (!autoRedirect)
                        throw;
                    if (viaNode == targetNode)
                    {
                        return await QuerySuccessor(key);
                    }
                    else
                    {
                        return await QuerySuccessor(key, viaNode);
                    }
                }
            }
        }
        public Task<Node> QuerySuccessor(string key) => QuerySuccessor(Hash(key));
        public Task<Node> QuerySuccessor(Node node) => QuerySuccessor(node.Hash);

        public Task JoinNetwork(string nodeToJoin) => JoinNetwork(MakeNode(nodeToJoin));
        public async Task JoinNetwork(Node nodeToJoin)
        {
            if (PredecessorNode != Node)
            {
                throw new InvalidOperationException("Already part of a network");
            }

            Logger.Info($"Joining network at {nodeToJoin}");

            List<Node> attemptedJoins = new List<Node>();

            for (int attempt = 0; attempt < 5; attempt++)
            {
                QuerySuccessorCache.Clear();

                // Which node should handle this join?
                var joiningTo = await QuerySuccessor(Node.Hash, nodeToJoin);

                if (joiningTo == Node)
                {
                    // If QuerySuccessor returns the current node, it means the node to join was down and QuerySuccessor fell back to the successor
                    Logger.Warn($" - node {nodeToJoin} seems to be down");
                    throw new RejectedException($"Unable to join via node {nodeToJoin}, node down?");
                }

                Logger.Info($" - node {nodeToJoin} suggests successor {joiningTo}");

                if (attemptedJoins.Contains(joiningTo))
                {
                    Logger.Warn($" - already tried to join at {joiningTo}");
                    continue;
                }
                attemptedJoins.Add(joiningTo);
                Logger.Info($" - Found successor {joiningTo} via {nodeToJoin}, requesting to join...");

                try
                {
                    var joinRequested = await SendMessageAsync(joiningTo, new JoinNetwork());
                    Logger.Info($" - Joining with predecessor {joinRequested.PredecessorNode} and successor {joinRequested.SuccessorNode}, building finger table...");

                    PredecessorNode = joinRequested.PredecessorNode;
                    SuccessorNode = joinRequested.SuccessorNode;
                    SetSuccessorsList(joinRequested.Successors);

                    await BuildFingerTable();
                    Logger.Notice($" - Successfully joined network via node {joiningTo}");
                    return;
                }
                catch (Exception e)
                {
                    Logger.Warn($" - Unable to join at successor {joiningTo}");
                }

                // Ask the rejecting node to provide us with our successor
                nodeToJoin = joiningTo;
                Logger.Info($" - Waiting 0.5 second before trying again");
                await Task.Delay(500);
            }
            throw new RejectedException($"Unable to join, tried nodes {string.Join(", ", attemptedJoins.Select(n => n.Name))}");
        }

        public async Task LeaveNetwork()
        {
            try
            {
                await SendMessageAsync(PredecessorNode, new LeavingNetwork(Node, SuccessorNode, PredecessorNode));
            }
            catch (Exception)
            {
                // Don't care if this fails, we'll let the network clean up the mess after we're gone
            }

            try
            {
                await SendMessageAsync(SuccessorNode, new LeavingNetwork(Node, SuccessorNode, PredecessorNode));
            }
            catch (Exception)
            {
                // Don't care if this fails, we'll let the network clean up the mess after we're gone
            }

            lock (Finger)
            {
                PredecessorNode = Node;
                SuccessorNode = Node;

                // Clear our finger table
                for (int i = 0; i < FingerCount; i++)
                {
                    Finger[i] = Node;
                }
            }
        }


        /// <summary>
        /// Detach immediately from the network, removing all knowledge about other nodes without notifying
        /// nodes about that.
        /// </summary>
        public void Detach()
        {
            lock (Finger)
            {
                PredecessorNode = Node;
                SuccessorNode = Node;
                for (int i = 0; i < FingerCount; i++)
                {
                    Finger[i] = Node;
                }
            }
        }

        public Node[] GetKnownNodes()
        {
            return Finger.Distinct().ToArray();
        }

        /// <summary>
        /// Runs periodic stabilization between nodes. Currently fixed at a 23 second intervals.
        /// </summary>
        /// <returns></returns>
        public async Task RunStabilization()
        {
            if (PredecessorNode == Node)
            {
                // Not networked, so no stabilization
                return;
            }

            // Logger.Debug("Running stabilization");

            bool needFingerTableRebuild = false;

            /**
             * Verify our predecessor node status. Consider it failed if it does not respond.
             */
            try
            {
                // Check that the predecessor is alive
                if (PredecessorNode != Node)
                {
                    // Ping our predecessor
                    var predecessor = await SendMessageAsync(PredecessorNode, new RequestNodeInfo());
                    if (PredecessorFailed)
                    {
                        Logger.Notice($"Predecessor {PredecessorNode} has recovered");
                    }
                }
                PredecessorFailed = false;
            }
            catch (NetworkException)
            {
                if (!PredecessorFailed)
                {
                    Logger.Warn($"Predecessor {PredecessorNode} marked as failed");
                }
                PredecessorFailed = true;
            }

            /**
             * Verify our successor to ensure building the finger table succeeds
             */
            try
            {
                if (PredecessorNode == SuccessorNode && !PredecessorFailed)
                {
                    // In a two node network, we don't need to also check the successor if the predecessor is ok
                }
                else if (PredecessorNode != Node)
                {
                    var successor = await SendMessageAsync(SuccessorNode, new RequestNodeInfo());
                    if (SuccessorFailed)
                    {
                        Logger.Notice($"Successor {SuccessorNode} has recovered");
                    }

                    SetSuccessorsList(successor.Successors);
                }
                SuccessorFailed = false;
            }
            catch (NetworkException)
            {
                /**
                 * Our successor seems to have failed, so we should try to replace it.
                 */
                if (!SuccessorFailed)
                {
                    Logger.Warn($"Successor {SuccessorNode} marked as failed");
                }
                SuccessorFailed = true;

                /**
                 * Make a backup successor become our successor
                 */
                if (!await UpdateLock.AcquireAsync(TimeSpan.FromSeconds(10)))
                {
                    Logger.Debug("Unable to aquire lock to update successor");
                    return;
                }

                try
                {
                    foreach (var backupSuccessor in SuccessorsList)
                    {
                        if (backupSuccessor == SuccessorNode)
                        {
                            // skip this, since it was tested before we came here
                            continue;
                        }
                        try
                        {
                            var failedSuccessor = SuccessorNode;
                            var acceptedAsPredecessor = await SendMessageAsync(backupSuccessor, new RequestAsSuccessor(SuccessorNode));
                            Logger.Notice($"Backup successor {backupSuccessor} will replace previous successor {SuccessorNode}");
                            SuccessorNode = backupSuccessor;
                            SuccessorFailed = false;
                            SetSuccessorsList(acceptedAsPredecessor.Successors);
                            SuccessorsList.Remove(failedSuccessor);
                            break;
                        }
                        catch (RejectedException)
                        {
                            Logger.Notice($"Backup successor {backupSuccessor} rejected our request to make it this nodes' successor");
                            break;
                        }
                        catch (Exception)
                        {
                            Logger.Warn($"Backup successor {backupSuccessor} did not respond");
                        }
                    }
                }
                finally
                {
                    UpdateLock.Release();
                }
            }

            if (PredecessorFailed || SuccessorFailed || Util.RandomInt(0, 100) < 20)
            {
                await BuildFingerTable();
            }
        }

        /// <summary>
        /// Builds the finger table by only querying other nodes and not relying on own data
        /// </summary>
        /// <param name="successor">The node to start querying from (defaults to SuccessorNode)</param>
        /// <returns></returns>
        public async Task BuildFingerTable()
        {
            FingerTableAddCache.Clear();
            if (SuccessorFailed)
            {
                Logger.Warn("Can't build finger table when successor is marked as failed");
                return;
            }
            Node currentSuccessor = SuccessorNode;
            Node[] newFingerTable = (Node[])Finger.Clone();

            for (int i = 0; i < FingerCount; i++)
            {
                // The start offset for the finger
                ulong start = Node.Hash + Util.Pow(2, (ulong)i);

                if (Util.Inside(currentSuccessor.Hash, start, Node.Hash))
                {
                    // The previous finger can be used for this finger table entry as well
                    newFingerTable[i] = currentSuccessor;
                    continue;
                }

                // Query Successor is more expensive than 

                currentSuccessor = await QuerySuccessor(start, currentSuccessor);
                newFingerTable[i] = currentSuccessor;
            }
            
            lock (Finger)
            {
                Finger = newFingerTable;
            }
        }

        /// <summary>
        /// Calculate the hash value of a byte array using the injected hash function.
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        public ulong Hash(byte[] key) => HashFunction(key);
        public ulong Hash(string key) => HashFunction(Encoding.UTF8.GetBytes(key));

        async Task<TResponse> SendMessageAsync<TResponse>(Node to, IRequest<TResponse> message)
            where TResponse : IResponse
        {
            if (Node == to)
            {
                // throw new InvalidOperationException("Trying to send a message to myself");
            }

            message.Sender = Node;
            message.Receiver = to;

            var result = await NetworkAdapter.SendMessageAsync(message);
            FingerTableAdd(to);
            return message.Filter(result);
        }

        /// <summary>
        /// Find the finger pair where preceding finger < key and succeeding finger >= key
        /// </summary>
        /// <param name="keyHash"></param>
        /// <returns></returns>
        public (Node, Node?) FingerTableQuery(ulong keyHash)
        {
            lock (Finger)
            {
                var previous = Node;
                for (int i = 0; i < FingerCount; i++)
                {
                    if (Util.Inside(keyHash, previous.Hash + 1, Finger[i].Hash))
                    {
                        return (previous, Finger[i]);
                    }
                    previous = Finger[i];
                }
                if (Finger[FingerCount - 1] == PredecessorNode)
                {
                    return (Finger[FingerCount - 1], Node);
                }
                return (Finger[FingerCount - 1], null);
            }
        }

        /// <summary>
        /// Immediately remove a node from the finger table to avoid routing to it
        /// </summary>
        /// <param name="nodeToDelete"></param>
        public void FingerTableRemove(Node nodeToDelete)
        {
            FingerTableAddCache.Clear();

            Node[] newFingerTable = (Node[])Finger.Clone();

            Node? replacementNode = null;

            for (int i = 0; i < FingerCount; i++)
            {
                if (replacementNode == null)
                {
                    replacementNode = Finger[i];
                }
                if (newFingerTable[i] == nodeToDelete)
                {
                    newFingerTable[i] = replacementNode;
                }
                else
                {
                    newFingerTable[i] = Finger[i];
                    replacementNode = Finger[i];
                }
            }

            Finger = newFingerTable;
        }

        /// <summary>
        /// Immediately add a node to the finger table, bypassing stabilization
        /// </summary>
        /// <returns>Actualy number of times the node was added</returns>
        /// <param name="nodeToAdd"></param>
        public int FingerTableAdd(Node nodeToAdd)
        {
            if (FingerTableAddCache.ContainsKey(nodeToAdd, true))
            {
                return 0;
            }
            FingerTableAddCache.Set(nodeToAdd, nodeToAdd);
            int count = 0;
            Node[] newFingerTable = (Node[])Finger.Clone();

            for (int i = 0; i < FingerCount; i++)
            {
                // The start offset for the finger
                ulong start = Node.Hash + Util.Pow(2, (ulong)i);

                if (nodeToAdd != Finger[i] && Util.Inside(nodeToAdd.Hash, start, Finger[i].Hash))
                {
                    count++;
                    // The node is a better finger
                    newFingerTable[i] = nodeToAdd;
                    continue;
                }
            }

            lock (Finger)
            {
                Finger = newFingerTable;
            }

            return count;
        }

        public Node MakeNode(string nodeName)
        {
            return new Node(nodeName, Hash(nodeName));
        }      
        
        private void SetSuccessorsList(List<Node> successors)
        {
            List<Node> newSuccessors = new List<Node>();
            foreach (Node node in successors)
            {
                if (node == Node)
                {
                    break;
                }
                newSuccessors.Add(node);
                if (newSuccessors.Count >= MaxSuccessorCount)
                {
                    break;
                }
            }
            SuccessorsList = newSuccessors;
        }

        private List<Node> MakeSuccessorsList()
        {
            List<Node> successors = new List<Node>() { Node };
            foreach (Node successor in SuccessorsList)
            {
                if (successors.Contains(successor))
                {
                    return successors;
                }
                successors.Add(successor);
                if (successors.Count >= MaxSuccessorCount)
                {
                    break;
                }
            }
            return successors;
        }

        private string DumpFingersTable()
        {
            StringBuilder sb = new StringBuilder();

            lock (Finger)
            {
                ulong a = 1;
                for (ulong i = 0; i < FingerCount; i++)
                {
                    ulong from = Node.Hash + a;
                    a *= 2;
                    sb.AppendLine($"{i,2} from={Util.Percent(from)}% ({from,22}) {Finger[i]}");
                }
                return sb.ToString();
            }
        }

        public string GetDebugInfo()
        {
            var sb = new StringBuilder();
            sb.AppendLine($"DEBUG INFO FOR NODE {Node}");
            sb.AppendLine($"================================================\n");

            if (PredecessorNode != Node)
            {
                sb.AppendLine($"Networked");
            }
            else
            {
                sb.AppendLine($"NOT networked");
            }

            sb.AppendLine(
                $"\n" +
                $"Predecessor: {PredecessorNode} {(PredecessorFailed ? "FAILED" : "")}\n" +
                $"Successor: {SuccessorNode} {(SuccessorFailed ? "FAILED" : "")}\n" +
                $"Successors list: {string.Join(", ", SuccessorsList)}\n" +
                $"\n" +
                $"Fingers Table:\n" +
                $"--------------\n" +
                $"{DumpFingersTable()}\n");
            return sb.ToString();
        }

    }
}


