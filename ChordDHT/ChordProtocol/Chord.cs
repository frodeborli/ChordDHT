using ChordDHT.ChordProtocol;
using ChordDHT.ChordProtocol.Exceptions;
using ChordDHT.ChordProtocol.Messages;
using ChordDHT.DHT;
using ChordDHT.Fubber;
using Fubber;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Reflection;
using System.Runtime.CompilerServices;
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

        private int SuccessorFailedCounter = 0;

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

        public DateTime LastFingerTableChange { get; private set; } = DateTime.UtcNow;

        public DateTime LastFingerTableCheck { get; private set; } = DateTime.UtcNow;

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

        private SerialExecutor NeighborManager = new SerialExecutor();

        private DateTime? slowdownDetected = null;


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
            NetworkAdapter.SetChord(this);

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
        }

        private void SetupMessageHandlers()
        {
            NetworkAdapter.AddHandler(async (InformNodeGone request) => {
                // Confirm that node is in fact gone
                try
                {
                    var result = await RequestNodeInfo(request.GoneNode);
                    throw new RejectedException("Node is not gone");
                }
                catch (Exception e)
                {
                    Logger.Error($"Exception: {e}");
                    // Node is in fact gone
                    return await NeighborManager.Serial(() => {
                        Logger.Info($"Was informed that {request.GoneNode} is gone");
                        var (predecessorNode, successorNode) = GetNeighbors();
                        if (request.GoneNode == predecessorNode)
                        {
                            PredecessorFailed = true;
                        }
                        if (request.GoneNode == successorNode)
                        {
                            SuccessorFailed = true;
                        }
                        FingerTableRemove(request.GoneNode);
                        return Task.FromResult(AcknowledgeReply.Instance);
                    });
                }
            });
            NetworkAdapter.AddHandler((FindPredecessor request) =>
            {
                var (_, successorNode) = GetNeighbors();
                if (Util.Inside(request.Hash, Node.Hash + 1, successorNode.Hash))
                {
                    return Task.FromResult(new FindPredecessorReply(Node, false));
                }
                var (predecessor, successor) = FingerTableQuery(request.Hash);
                return Task.FromResult(new FindPredecessorReply(predecessor, true));
            });
            NetworkAdapter.AddHandler((FindSuccessor request) => {
                var (predecessorNode, successorNode) = GetNeighbors();
                if (Util.Inside(request.Hash, Node.Hash + 1, successorNode.Hash))
                {
                    if (SuccessorFailed)
                    {
                        // Won't route to my failed successor
                        if (!PredecessorFailed)
                        {
                            // Using predecessor if not down
                            return Task.FromResult(new FindSuccessorReply(predecessorNode, false));
                        }
                        foreach (var finger in Finger)
                        {
                            // Using first finger that is not our successor, ourselves or our predecessor
                            if (finger != SuccessorNode && finger != Node && finger != PredecessorNode)
                            {
                                return Task.FromResult(new FindSuccessorReply(predecessorNode, false));
                            }
                        }
                    }
                    return Task.FromResult(new FindSuccessorReply(successorNode, false));
                }
                var (predecessor, successor) = FingerTableQuery(request.Hash);
                return Task.FromResult(new FindSuccessorReply(predecessor, true));
            });
            NetworkAdapter.AddHandler(async (BecomeMySuccessor request) =>
            {
                Logger.Info($"Node {request.Sender} is asking us to become its successor");

                // Making the test outside of the NeighborManager, to avoid locking if possible
                var (predecessorNode, predecessorFailed, successorNode, successorFailed) = GetNeighborsWithState();
                if (!predecessorFailed)
                {
                    try
                    {
                        var bestSuccessor = await FindSuccessor(request.Sender.Hash + 1);
                        if (bestSuccessor == Node)
                        {
                            // Special case. A predecessor is trying to become my predecessor, but I already have a worse predecessor. I should replace my predecessor
                            // as if we are handling a join.
                            return new BecomeMySuccessorReply(null, null);
                        }
                        else
                        {
                            Logger.Notice($" - Rejecting new predecessor {request.Sender} since our current predecessor {predecessorNode} is not marked as failed, suggesting {bestSuccessor} instead");
                        }
                        return new BecomeMySuccessorReply(bestSuccessor);
                    }
                    catch
                    {
                        Logger.Notice($" - Rejecting new predecessor {request.Sender} since our current predecessor {predecessorNode} is not marked as failed, suggesting our own predecessor instead");
                        return new BecomeMySuccessorReply(predecessorNode);
                    }                    
                }

                return await NeighborManager.Serial(() =>
                {
                    if (PredecessorNode == Node)
                    {
                        // Seem to have left the network so we must reject
                        throw new RejectedException("I have left the network");
                    }
                    var (predecessorNode, predecessorFailed, successorNode, successorFailed) = GetNeighborsWithState();
                    if (predecessorFailed)
                    {
                        Logger.Info($" - Accepting a new predecessor {request.Sender} since our current predecessor {predecessorNode} is failed");
                        SetNeighbors(predecessor: request.Sender, predecessorFailed: false);
                        LogState();
                        return Task.FromResult(new BecomeMySuccessorReply(SuccessorsList));
                    }
                    else
                    {
                        Logger.Notice($" - Rejecting new predecessor {request.Sender} since our current predecessor {predecessorNode} is not marked as failed");
                        return Task.FromResult(new BecomeMySuccessorReply(predecessorNode));
                    }
                });
            });
            NodeInfoReply cachedNodeInfoReply = new NodeInfoReply(PredecessorNode, SuccessorNode, SuccessorsList, Node.Hash, Node.Name);
            NetworkAdapter.AddHandler((RequestNodeInfo request) =>
            {
                FingerTableAdd(request.Sender);
                var (predecessorNode, successorNode) = GetNeighbors();
                cachedNodeInfoReply.PredecessorNode = predecessorNode;
                cachedNodeInfoReply.SuccessorNode = successorNode;
                cachedNodeInfoReply.Successors = SuccessorsList;
                return Task.FromResult(cachedNodeInfoReply); // new NodeInfoReply(predecessorNode, successorNode, SuccessorsList, Node.Hash, Node.Name));

                /*
                return await NeighborManager.Serial(() =>
                {
                    return Task.FromResult(new NodeInfoReply(PredecessorNode, SuccessorNode, MakeSuccessorsList(), Node.Hash, Node.Name));
                });
                */
            });

            NetworkAdapter.AddHandler(async (LeavingNetwork request) =>
            {
                /**
                 * When a predecessor tells us that it is leaving
                 */
                Logger.Info($"Node {request.Sender} is telling us that it is leaving");

                // try to avoid NeighborManager
                var (predecessorNode, successorNode) = GetNeighbors();
                if (request.LeavingNode == successorNode)
                    return AcknowledgeReply.Instance;

                // Random delay to help with bursts
                // await Task.Delay(Util.RandomInt(100, 1000));

                return await NeighborManager.Serial(async () =>
                {
                    var (predecessorNode, successorNode) = GetNeighbors();
                    if (request.LeavingNode == successorNode)
                    {
                        // Our successor is leaving, ignore

                    }
                    else if (request.LeavingNode == predecessorNode)
                    {
                        // Predecessor is leaving
                        await SendMessageAsync(request.PredecessorNode, new YouHaveNewSuccessor(Node, SuccessorsList));
                        SetNeighbors(predecessor: request.PredecessorNode, predecessorFailed: false);
                        FingerTableRemove(request.Sender);
                    }

                    return AcknowledgeReply.Instance;
                });
            });
            
            NetworkAdapter.AddHandler(async (YouHaveNewSuccessor request) =>
            {
                /**
                 * When a successor tells us that we have a new successor (after a join)
                 */
                Logger.Info($"Node {request.Sender} is telling us that we have a new successor {request.SuccessorNode}");

                return await NeighborManager.Serial(() =>
                {
                    if (PredecessorNode == Node)
                    {
                        throw new RejectedException("Won't accept this successor since I'm not part of a network");
                    }
                    SetNeighbors(successor: request.SuccessorNode, successorFailed: false);
                    var newSuccessorList = request.Successors;
                    newSuccessorList.Insert(0, Node);
                    SuccessorsList = newSuccessorList.GetRange(0, Math.Min(MaxSuccessorCount, newSuccessorList.Count));                    
                    Logger.Ok($"NotifyNewSuccessor-handler: Accepted new successor {request.SuccessorNode} from {request.Sender}");
                    LogState();
                    return Task.FromResult(AcknowledgeReply.Instance);
                });
            });

            NetworkAdapter.AddHandler(async (JoinNetwork request) => 
            {
                /**
                 * When a node request to join the network:
                 * 
                 *  - Run serialized
                 *  - Hold the connection until everything is OK, or restore state back to original state if not
                 *  - Make all arrangements here, and notify the predecessor about having a new successor
                 */
                // Random delay to help with bursts
                // await Task.Delay(Util.RandomInt(100, 1000));
                var (predecessorNode, successorNode) = GetNeighbors();
                if (request.Sender == predecessorNode)
                {
                    Logger.Error("Our predecessor is trying to rejoin here!!!");

                }
                if (!Util.Inside(request.Sender!.Hash, predecessorNode.Hash + 1, Node.Hash))
                {
                    // Requester is not trying to join in
                    Logger.Info($" - Rejecting join for {request.Sender} because it is not in my key space range from {predecessorNode} to {Node}");
                    throw new RejectedException("Must join at the correct successor");
                }
                else if (PredecessorFailed)
                {
                    Logger.Info($" - Rejecting join for {request.Sender} because of a failed predecessor");
                    throw new RejectedException("Temporarily can't accept joins");
                }

                return await NeighborManager.Serial(async () => {

                    JoinNetworkReply response;

                    var (predecessorNode, successorNode) = GetNeighbors();

                    Node? setSuccessorNode = null;
                    bool? setSuccessorFailed = null;

                    if (predecessorNode == Node)
                    {
                        setSuccessorNode = request.Sender;
                        setSuccessorFailed = false;
                        response = new JoinNetworkReply(Node, Node, SuccessorsList);
                    }
                    else
                    {
                        // Make the predecessors successor list
                        var preSuccessors = new List<Node>(SuccessorsList);
                        preSuccessors.Insert(0, request.Sender);
                        var predecessorSaid = await SendMessageAsync(PredecessorNode, new YouHaveNewSuccessor(request.Sender, preSuccessors));
                        response = new JoinNetworkReply(PredecessorNode, Node, SuccessorsList);
                    }
                    SetNeighbors(
                        predecessor: request.Sender,
                        predecessorFailed: false,
                        successor: setSuccessorNode,
                        successorFailed: setSuccessorFailed
                        );

                    Logger.Ok("Node handling the join is rebuilding the finger table");
                    FingerTableAdd(request.Sender);
                    //NextFingerToRebuild = 0;
                    //await BuildFingerTable(FingerCount);

                    return response;
                });
            });


        }

        /// <summary>
        /// Find a successor without using our own finger table
        /// </summary>
        /// <param name="key"></param>
        /// <param name="viaNode"></param>
        /// <returns></returns>
        /// <exception cref="Exception"></exception>
        public async Task<Node> FindSuccessor(ulong key, Node viaNode, int maxHops = 200)
        {
            var (predecessorNode, predecessorFailed, successorNode, successorFailed) = GetNeighborsWithState();
            FindSuccessorReply joinLookup;
            int hopCount = 0;
            while (true)
            {
                if (hopCount++ > maxHops)
                {
                    throw new MaxHopCountReached();
                }
                if (hopCount > 20)
                {
                    // Introduce random delay because it seems the finger table needs to update somewhere
                    await Task.Delay(Util.RandomInt(100, 400));
                }
                joinLookup = await SendMessageAsync(viaNode, new FindSuccessor(key));
                if (!joinLookup.IsRedirect)
                {
                    return new Node(joinLookup.Node, hopCount);
                }
                if (joinLookup.Node == Node)
                {
                    Logger.Warn("FindSuccessor via redirected back to us!");
                    // We were redirected back to ourselves, try various remedies
                    if (!successorFailed)
                    {
                        Logger.Error("Going via our successor node which is not failed");
                        viaNode = successorNode;
                    }
                    else if (SuccessorsList.Count > 1)
                    {
                        Logger.Error("Going via our backup successor node");
                        viaNode = SuccessorsList[1];
                    }
                    else if (!predecessorFailed)
                    {
                        Logger.Error("Going via our predecessor which is not failed");
                        viaNode = predecessorNode;
                    }
                    else
                    {
                        throw new Exception("UNABLE TO FIND WITHOUT ASKING OURSELVES");
                    }
                }
                else
                {
                    viaNode = joinLookup.Node;
                }
            }
        }

        public async Task<Node> FindSuccessor(ulong key, int maxHops = 200)
        {
            int hopCount = 0;
            if (Util.Inside(key, Node.Hash + 1, SuccessorNode.Hash))
            {
                return new Node(SuccessorNode, hopCount);
            }
            var (predecessorNode, predecessorFailed, successorNode, successorFailed) = GetNeighborsWithState();
            var (predecessor, successor) = FingerTableQuery(key);
            Node? previousNode = null;
            StartOver:
            while (true)
            {
                hopCount++;
                if (hopCount > maxHops)
                {
                    throw new MaxHopCountReached();
                }
                if (hopCount > 20)
                {
                    // Introduce random delay because it seems the finger table needs to update somewhere
                    await Task.Delay(Util.RandomInt(100, 400));
                }
                FindSuccessorReply result;
                try
                {
                    if (predecessor == SuccessorNode && SuccessorFailed)
                    {
                        if (!predecessorFailed)
                        {
                            predecessor = PredecessorNode;
                        }
                        else
                        {
                            predecessor = successor;
                        }
                    }
                    result = await SendMessageAsync(predecessor, new FindSuccessor(key));
                    previousNode = predecessor;
                }
                catch (NetworkException e)
                {
                    Logger.Error($"NetworkException: {e}");
                    if (previousNode == null)
                    {
                        Logger.Warn($"We found a node {predecessor} that was down in our own finger table");
                        if (predecessor == predecessorNode)
                        {
                            Logger.Error("Our predecessor is down, not sure if we must do anything with that here");
                            SetNeighbors(predecessorFailed: true);
                        }
                        else if (predecessor == successorNode)
                        {
                            Logger.Error("Our successor is down, not sure if we must do anything with that here");
                            SetNeighbors(successorFailed: true);
                        }
                        // we were not redirected, this node was in our finger table
                        FingerTableRemove(predecessor);

                        // find the new predecessor from the finger table
                        (predecessor, _) = FingerTableQuery(key);
                        goto StartOver;
                    }
                    else
                    {
                        // We were redirected by a previous result
                        Logger.Warn($"Informing {previousNode} that {predecessor} is gone");
                        try
                        {
                            await SendMessageAsync(previousNode, new InformNodeGone(predecessor));
                        }
                        catch (Exception ex)
                        {
                            Logger.Error($"Exception: {ex}");
                        }
                        Logger.Warn($"Repeating request to {previousNode}");
                        predecessor = previousNode;
                        goto StartOver;
                    }
                }
                
                if (!result.IsRedirect)
                {
                    if (hopCount > 50)
                    {
                        Logger.Warn($"Very high hop count {hopCount}");
                    }
                    return new Node(result.Node, hopCount);
                }
                predecessor = result.Node;
            }
        }
        public async Task<Node> FindPredecessor(ulong key)
        {
            int hopCount = 0;
            if (Util.Inside(key, Node.Hash + 1, SuccessorNode.Hash))
            {
                return new Node(Node, hopCount);
            }
            var (predecessor, successor) = FingerTableQuery(key);
            while (true)
            {
                hopCount++;
                var result = await SendMessageAsync(predecessor, new FindPredecessor(key));
                if (!result.IsRedirect)
                {
                    return new Node(result.Node, hopCount);
                }
                predecessor = result.Node;
                if (hopCount > 100)
                {
                    throw new MaxHopCountReached();
                }
            }
        }

        public async Task<NodeInfoReply> RequestNodeInfo(Node to)
        {
            var sw = Stopwatch.StartNew();
            try
            {
                return await SendMessageAsync(to, new ChordDHT.ChordProtocol.Messages.RequestNodeInfo());
            }
            catch (NetworkException ex)
            {
                FingerTableRemove(to);
                throw;
            }
            finally
            {
                sw.Stop();
                if (sw.ElapsedMilliseconds > 100)
                {
                    slowdownDetected = DateTime.UtcNow + TimeSpan.FromSeconds(10);
                }
            }
        }

        public Task JoinNetwork(string nodeToJoin) => JoinNetwork(MakeNode(nodeToJoin));
        public async Task JoinNetwork(Node nodeToJoin)
        {
            if (PredecessorNode != Node)
            {
                throw new InvalidOperationException("Already part of a network");
            }

            Logger.Info($"Joining network at {nodeToJoin}");

            await NeighborManager.Serial(async () =>
            {
                var joiningTo = await FindSuccessor(Node.Hash, nodeToJoin);

                if (joiningTo == Node)
                {
                    // If QuerySuccessor returns the current node, it means the node to join was down and QuerySuccessor fell back to the successor
                    Logger.Warn($" - node {nodeToJoin} seems to be down");
                    throw new RejectedException($"Unable to join via node {nodeToJoin}, node down?");
                }

                try
                {
                    var joinRequested = await SendMessageAsync(joiningTo, new JoinNetwork());

                    SetNeighbors(predecessor: joinRequested.PredecessorNode, predecessorFailed: false, successor: joinRequested.SuccessorNode, successorFailed: false);
                    var newSuccessorList = joinRequested.Successors;
                    newSuccessorList.Insert(0, Node);
                    SuccessorsList = newSuccessorList.GetRange(0, Math.Min(MaxSuccessorCount, newSuccessorList.Count));
                    Logger.Ok("Joining node is rebuilding the finger table");
                    FingerTableAdd(joinRequested.PredecessorNode);
                    FingerTableAdd(joinRequested.SuccessorNode);
                    foreach (var s in joinRequested.Successors)
                    {
                        FingerTableAdd(s);
                    }
                    LogState();
                    return;
                }
                catch (Exception ex)
                {
                    Logger.Warn($" - Unable to join at successor {joiningTo}");
                    throw new RejectedException($"Unable to join at successor {joiningTo}\n{ex}");
                }

            });


        }

        public async Task LeaveNetwork()
        {
            try
            {
                await SendMessageAsync(SuccessorNode, new LeavingNetwork(Node, SuccessorNode, PredecessorNode));
            }
            catch (Exception e)
            {
                Logger.Error($"Exception: {e}");
                // Don't care if this fails, we'll let the network clean up the mess after we're gone
            }

            await ResetState();
        }

        public async Task ResetState()
        {

            await NeighborManager.Serial(() =>
            {
                SetNeighbors(Node, Node, false, false);
                SuccessorsList = new List<Node>() { Node };
                LastFingerTableChange = DateTime.UtcNow;

                var finger = new Node[FingerCount];

                // Clear our finger table
                for (int i = 0; i < FingerCount; i++)
                {
                    finger[i] = Node;
                }

                Finger = finger;

                LogState();

                return Task.CompletedTask;
            });
        }

        public FingerTableEntry[] GetFingerTable()
        {
            List<FingerTableEntry> fingerTable = new List<FingerTableEntry>();
            ulong i = 0;
            foreach (var finger in Finger)
            {
                fingerTable.Add(new FingerTableEntry(Node.Hash + Util.Pow(2, i++), finger));
            }
            return fingerTable.ToArray();
        }

        public Node[] GetKnownNodes()
        {
            var nodes = new List<Node>(Finger.Distinct());

            var (predecessorNode, successorNode) = GetNeighbors();

            if (!nodes.Contains(predecessorNode))
            {
                nodes.Add(predecessorNode);
            }
            if (!nodes.Contains(successorNode))
            {
                nodes.Add(successorNode);
            }
            foreach (var node in SuccessorsList)
            {
                if (!nodes.Contains(node))
                {
                    nodes.Add(node);
                }
            }

            return nodes.ToArray();
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

            // Stabilize predecessor, but skip if we can't get a lock on it
            var (predecessorNode, predecessorFailed, successorNode, successorFailed) = GetNeighborsWithState();
            try
            {
                var predecessor = await RequestNodeInfo(predecessorNode);
                if (predecessor.SuccessorNode != Node)
                {
                    Logger.Warn($"Predecessor {predecessorNode} reports having a different successor {predecessor.SuccessorNode} than me {Node}");
                    SetNeighbors(predecessorFailed: true);
                }
                else
                {
                    SetNeighbors(predecessorFailed: false);
                }
            }
            catch (Exception ex)
            {
                Logger.Warn($"Predecessor did not respond:\n{ex}");
                SetNeighbors(predecessorFailed: true);
            }

            (predecessorNode, predecessorFailed, successorNode, successorFailed) = GetNeighborsWithState();
            try
            {
                var successor = await RequestNodeInfo(successorNode);
                var newSuccessorList = successor.Successors;
                newSuccessorList.Insert(0, Node);
                SuccessorsList = newSuccessorList.GetRange(0, Math.Min(MaxSuccessorCount, newSuccessorList.Count));
                if (successor.PredecessorNode != Node)
                {
                    Logger.Warn($"Successor {successorNode} reports having a different predecessor {successor.PredecessorNode} than me {Node}");
                    SetNeighbors(successorFailed: true);                                                                    
                }
                else
                {
                    SetNeighbors(successorFailed: false);
                }
            }
            catch (Exception ex)
            {
                Logger.Warn($"Successor did not respond:\n{ex}");
                SetNeighbors(successorFailed: true);
            }


            if (SuccessorFailed)
            {
                (predecessorNode, predecessorFailed, successorNode, successorFailed) = GetNeighborsWithState();
                var rejoinCandidates = new Queue<Node>(SuccessorsList);
                if (!PredecessorFailed && !rejoinCandidates.Contains(PredecessorNode))
                {
                    rejoinCandidates.Enqueue(PredecessorNode);
                }
                while (rejoinCandidates.Count > 0)
                {
                    var backupNode = rejoinCandidates.Dequeue();
                    if (backupNode == Node || backupNode == SuccessorNode)
                        continue;

                    Logger.Warn($"Trying to replace successor with {backupNode}");

                    try
                    {
                        var nodeInfo = await RequestNodeInfo(backupNode);
                        bool shouldBreak = false;
                        var result = await SendMessageAsync(backupNode, new BecomeMySuccessor(SuccessorNode));
                        if (result.Successors != null)
                        {
                            Logger.Ok($"We have a new successor {backupNode}");
                            // The node accepted to become our successor
                            SetNeighbors(successor: backupNode, successorFailed: false);
                            var newSuccessorList = result.Successors;
                            newSuccessorList.Insert(0, Node);
                            SuccessorsList = newSuccessorList.GetRange(0, Math.Min(MaxSuccessorCount, newSuccessorList.Count));
                            SuccessorFailedCounter = 0;
                            shouldBreak = true;
                        }
                        else if (result.BetterPredecessor != null)
                        {
                            Logger.Ok($"--------------- ADDING SUGGESTED {result.BetterPredecessor} ---------");
                            rejoinCandidates.Enqueue(result.BetterPredecessor);
                        }
                        else
                        {
                            // This successor indicates that we must rejoin the ring
                            var currentPredecessor = PredecessorNode;
                            var currentPredecessorFailed = PredecessorFailed;
                            await ResetState();
                            await JoinNetwork(backupNode);
                            Logger.Ok("################### HAVE WE REJOINED? ###################");
                        }

                        if (shouldBreak)
                        {
                            break;
                        }
                    }
                    catch (NetworkException ex)
                    {
                        // fallback successor failed, remove it from successorslist
                        SuccessorsList.Remove(backupNode);
                        FingerTableRemove(backupNode);
                        Logger.Ok($"--------------- REMOVING FAILED SUCCESSOR {backupNode} -----------");
                    }
                    catch (Exception ex)
                    {
                        Logger.Error($"Error trying to replace successor:\n{ex}");
                    }
                }

            }
            else
            {
                SuccessorFailedCounter = 0;
            }

            await BuildFingerTable();
        }

        private bool HasDetectedSlowdown()
        {
            return slowdownDetected != null && slowdownDetected < DateTime.UtcNow;
        }

        private Cache<ulong,Node> FingerCache = new Cache<ulong,Node>(64);

        /// <summary>
        /// Builds the finger table by only querying other nodes and not relying on own data
        /// </summary>
        /// <param name="successor">The node to start querying from (defaults to SuccessorNode)</param>
        /// <returns></returns>
        public async Task BuildFingerTable()
        {
            LastFingerTableCheck = DateTime.UtcNow;
            Node node = SuccessorNode;
            var finger = (Node[])Finger.Clone();

            for (int i = 0; i < FingerCount; i++)
            {
                ulong start = Node.Hash + Util.Pow(2, (ulong)i);
                if (!Util.Inside(node.Hash, start, Node.Hash))
                {
                    if (FingerCache.TryGetValue(start, out Node? cachedNode))
                    {
                        node = cachedNode;
                    }
                    else
                    {
                        try
                        {
                            var nodeInfo = await RequestNodeInfo(finger[i]);
                            if (!Util.Inside(nodeInfo.PredecessorNode!.Hash, start, Node.Hash))
                            {
                                // We have the correct finger
                                node = finger[i];
                                // Caching long because we are certain
                                FingerCache.Set(start, node, TimeSpan.FromMilliseconds(Util.RandomInt(3000, 10000)));
                            }
                            else if (HasDetectedSlowdown() || Util.RandomInt(0, 100) < 90)
                            {
                                // Node at the finger has a better predecessor so we use most of the time
                                node = nodeInfo.PredecessorNode;
                                // Caching short because we might not have found the best finger node yet
                                FingerCache.Set(start, node, TimeSpan.FromMilliseconds(Util.RandomInt(500, 2000)));
                            }
                            else
                            {
                                try
                                {
                                    node = await FindSuccessor(start, 20);
                                    FingerCache.Set(start, node, TimeSpan.FromMilliseconds(Util.RandomInt(3000, 10000)));
                                }
                                catch (MaxHopCountReached)
                                {
                                    node = nodeInfo.PredecessorNode;
                                }
                                // Caching long because we are certain
                            }
                        }
                        catch (Exception e)
                        {
                            Logger.Error($"Exception: {e}");
                            Logger.Warn($"Used FindSuccessor to update finger table");
                            try
                            {
                                node = await FindSuccessor(start, 20);
                                FingerCache.Set(start, node, TimeSpan.FromMilliseconds(Util.RandomInt(3000, 10000)));
                            }
                            catch (MaxHopCountReached)
                            {
                                break;
                            }
                            // Caching long because we are certain
                        }

                    }
                    
                }

                // Logger.Debug($"Finger {i} {Util.Percent(start)}: {node}");
                if (PredecessorNode == Node)
                {
                    // Stop updating finger table if we leave the network during finger table rebuild
                    return;
                }
                finger[i] = node;
            }
            Finger = finger;
            return;
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
            message.Sender = Node;

            var result = await NetworkAdapter.SendMessageAsync(to, message);
            result = message.Filter(result);
            return result;
        }

        /// <summary>
        /// Find the finger pair where preceding finger < key and succeeding finger >= key
        /// </summary>
        /// <param name="keyHash"></param>
        /// <returns></returns>
        public (Node, Node?) FingerTableQuery(ulong keyHash)
        {
            var finger = Finger;

            var previous = Node;
            for (int i = 0; i < FingerCount; i++)
            {
                if (Util.Inside(keyHash, previous.Hash + 1, finger[i].Hash))
                {
                    return (previous, finger[i]);
                }
                previous = finger[i];
            }
            if (finger[FingerCount - 1] == PredecessorNode)
            {
                return (finger[FingerCount - 1], Node);
            }
            return (finger[FingerCount - 1], null);
        }

        /// <summary>
        /// Immediately remove a node from the finger table to avoid routing to it
        /// </summary>
        /// <param name="nodeToDelete"></param>
        public void FingerTableRemove(Node nodeToDelete)
        {
            if (nodeToDelete == SuccessorNode)
            {
                // Will not remove the successor node from the finger table
                return;
            }

            Node[] newFingerTable = (Node[])Finger.Clone();
            Node replacementNode = SuccessorNode;

            for (int i = 0; i < FingerCount; i++)
            {
                if (newFingerTable[i] == nodeToDelete)
                {
                    newFingerTable[i] = replacementNode;
                }
                else
                {
                    replacementNode = newFingerTable[i];
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
            if (PredecessorNode == Node)
            {
                // Not adding new nodes when we are not networked
                return 0;
            }
            bool didChange = false;
            int count = 0;
            Node[] newFingerTable = (Node[])Finger.Clone();

            for (int i = 0; i < FingerCount; i++)
            {
                // The start offset for the finger
                ulong start = Node.Hash + Util.Pow(2, (ulong)i);
              
                var existingNode = newFingerTable[i].Hash - start;
                var candidateNode = nodeToAdd.Hash - start;
                if (candidateNode < existingNode && newFingerTable[i] != Node)
                {
                    newFingerTable[i] = nodeToAdd;
                    didChange = true;
                }
            }

            Finger = newFingerTable;

            if (didChange)
            {
                Logger.Ok("Finger table updated");
                LastFingerTableChange = DateTime.UtcNow;
            }

            return count;
        }

        public Node MakeNode(string nodeName)
        {
            return new Node(nodeName, Hash(nodeName));
        }      
       

        private string DumpFingersTable()
        {
            StringBuilder sb = new StringBuilder();

            ulong a = 1;
            for (ulong i = 0; i < FingerCount; i++)
            {
                ulong from = Node.Hash + a;
                a *= 2;
                sb.AppendLine($"{i,2} from={Util.Percent(from)}% ({from,22}) {Finger[i]}");
            }
            return sb.ToString();
        }

        private ulong RelativeDistance(Node node)
        {
            return node.Hash - Node.Hash;
        }

        private void LogState()
        {
            var (predecessorNode, successorNode) = GetNeighbors();
            Logger.Debug($"STATE {Node} predecessor={predecessorNode} successor={successorNode}");
        }

        private NodeList GetNodeList()
        {
            var (predecessorNode, predecessorFailed, successorNode, successorFailed) = GetNeighborsWithState();
            NodeList list = new NodeList(HashFunction);
            if (!predecessorFailed) list.Add(predecessorNode);
            if (!successorFailed) list.Add(successorNode);
            foreach (var failover in SuccessorsList)
            {
                list.Add(failover);
            }
            foreach (var finger in Finger)
            {
                list.Add(finger);
            }
            return list;
        }

        /**
         * Atomic get of one or both neighbors
         */
        private (Node, Node) GetNeighbors()
        {
            return (PredecessorNode, SuccessorNode);
        }

        private (Node, bool, Node, bool) GetNeighborsWithState()
        {
            return (PredecessorNode, PredecessorFailed, SuccessorNode, SuccessorFailed);
        }

        /**
         * Atomic set of one or both neighbors
         */
        private void SetNeighbors(Node? predecessor = null, Node? successor = null, bool? predecessorFailed = null, bool? successorFailed = null)
        {
            if (predecessor != null)
                PredecessorNode = predecessor;
            if (predecessorFailed != null)
                PredecessorFailed = (bool)predecessorFailed;
            if (successor != null)
                SuccessorNode = successor;
            if (successorFailed != null)
                SuccessorFailed = (bool)successorFailed;
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


