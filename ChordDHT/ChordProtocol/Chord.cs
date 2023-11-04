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

        private int NextFingerToRebuild = 0;

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
        }

        private void SetupMessageHandlers()
        {
            NetworkAdapter.AddHandler((FindPredecessor request) =>
            {
                if (Util.Inside(request.Hash, Node.Hash + 1, SuccessorNode.Hash))
                {
                    return Task.FromResult(new FindPredecessorReply(Node, false));
                }
                var (predecessor, successor) = FingerTableQuery(request.Hash);
                return Task.FromResult(new FindPredecessorReply(predecessor, true));
            });
            NetworkAdapter.AddHandler((FindSuccessor request) => {
                if (Util.Inside(request.Hash, Node.Hash + 1, SuccessorNode.Hash))
                {
                    return Task.FromResult(new FindSuccessorReply(SuccessorNode, false));
                }
                var (predecessor, successor) = FingerTableQuery(request.Hash);
                return Task.FromResult(new FindSuccessorReply(predecessor, true));
            });
            NetworkAdapter.AddHandler(async (BecomeMySuccessor request) =>
            {
                Logger.Info($"Node {request.Sender} is asking us to become its successor");

                return await NeighborManager.Serial(() =>
                {
                    if (PredecessorFailed)
                    {
                        Logger.Info($" - Accepting a new predecessor {request.Sender} since our current predecessor {PredecessorNode} is failed");
                        PredecessorNode = request.Sender;
                        PredecessorFailed = false;
                        LogState();
                        return Task.FromResult(new BecomeMySuccessorReply(MakeSuccessorsList()));
                    }
                    else
                    {
                        Logger.Notice($" - Rejecting new predecessor {request.Sender} since our current predecessor {PredecessorNode} is not marked as failed");
                        return Task.FromResult(new BecomeMySuccessorReply(PredecessorNode));
                    }
                });
            });
            NetworkAdapter.AddHandler((RequestNodeInfo request) =>
            {
                FingerTableAdd(request.Sender);
                return Task.FromResult(new NodeInfoReply(PredecessorNode, SuccessorNode, MakeSuccessorsList(), Node.Hash, Node.Name));

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

                // Random delay to help with bursts
                await Task.Delay(Util.RandomInt(100, 1000));

                return await NeighborManager.Serial(async () =>
                {
                    if (request.LeavingNode == SuccessorNode)
                    {
                        // Our successor is leaving, ignore

                    }
                    else if (request.LeavingNode == PredecessorNode)
                    {
                        // Predecessor is leaving
                        await SendMessageAsync(request.PredecessorNode, new YouHaveNewSuccessor(Node, MakeSuccessorsList()));
                        PredecessorNode = request.PredecessorNode;
                        PredecessorFailed = false;
                        FingerTableRemove(request.Sender);
                    }

                    return new AcknowledgeReply();
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
                    SuccessorNode = request.SuccessorNode;
                    SetSuccessorsList(request.Successors);
                    Logger.Ok($"NotifyNewSuccessor-handler: Accepted new successor {SuccessorNode} from {request.Sender}");
                    LogState();
                    return Task.FromResult(new AcknowledgeReply());
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

                return await NeighborManager.Serial(async () => {
                    if (!Util.Inside(request.Sender!.Hash, PredecessorNode.Hash + 1, Node.Hash))
                    {
                        // Requester is not trying to join in
                        Logger.Info($" - Rejecting join for {request.Sender} because it is not in my key space range from {PredecessorNode} to {Node}");
                        throw new RejectedException("Must join at the correct successor");
                    }
                    else if (PredecessorFailed)
                    {
                        Logger.Info($" - Rejecting join for {request.Sender} because of a failed predecessor");
                        throw new RejectedException("Temporarily can't accept joins");
                    }

                    JoinNetworkReply response;


                    if (PredecessorNode == Node)
                    {
                        SuccessorNode = request.Sender;
                        SuccessorFailed = false;
                        SetSuccessorsList(new List<Node> { SuccessorNode });
                        response = new JoinNetworkReply(Node, Node, MakeSuccessorsList());
                    }
                    else
                    {
                        var preSuccessors = MakeSuccessorsList();
                        preSuccessors.Insert(0, request.Sender);
                        var predecessorSaid = await SendMessageAsync(PredecessorNode, new YouHaveNewSuccessor(request.Sender, preSuccessors));
                        response = new JoinNetworkReply(PredecessorNode, Node, MakeSuccessorsList());
                    }
                    PredecessorNode = request.Sender;
                    PredecessorFailed = false;

                    Logger.Ok("Node handling the join is rebuilding the finger table");
                    FingerTableAdd(request.Sender);
                    //NextFingerToRebuild = 0;
                    //await BuildFingerTable(FingerCount);

                    return response;
                });

                /*

                if (!await UpdateLock.AcquireAsync(TimeSpan.FromSeconds(10)))
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
                    else if (HasJoiningPredecessor != null)
                    {
                        Logger.Info($"Rejecting join for {request.Sender} because we're handling another join ({HasJoiningPredecessor})");
                        throw new RejectedException("Another node is already joining here");
                    }
                    else if (HasJoiningSuccessor != null)
                    {
                        Logger.Info($"Rejecting join for {request.Sender} because we're handling another successor join ({HasJoiningPredecessor})");
                        throw new RejectedException("Another node is becoming our successor");
                    }
                    else if (JoiningPredecessor != null)
                    {
                        Logger.Info($"Rejecting join for {request.Sender} because we're waiting for ack from another join ({JoiningPredecessor})");
                        throw new RejectedException("Another node is already joining here");
                    }

                    if (JoinLockedUntil != null && JoinLockedUntil < DateTime.UtcNow)
                    {
                        Logger.Warn($"Not accepting joins just yet");
                        throw new RejectedException("Not accepting joins just yet");
                    }
                    JoinLockedUntil = DateTime.UtcNow + TimeSpan.FromSeconds(5);

                    if (PredecessorNode == Node)
                    {
                        JoiningPredecessor = request.Sender;
                        _ = Task.Run(async () =>
                        {
                            var sender = request.Sender;
                            await Task.Delay(10000);
                            if (JoiningPredecessor == sender)
                            {
                                Logger.Warn("Clearing JoiningPredecessor after timeout");
                                JoiningPredecessor = null;
                            }
                        });
                        // No need to notify about the new successor
                        return new JoinNetworkResponse(Node, Node, new List<Node> { request.Sender });
                    }

                    HasJoiningPredecessor = DateTime.UtcNow;

                    try
                    {
                        Logger.Notice($"Notifying predecessor {PredecessorNode} to prepare for a join");
                        var ack = await SendMessageAsync(PredecessorNode, new PrepareForSuccessor());
                        Logger.Notice($"Predecessor {PredecessorNode} accepted to prepare for a join");
                    }
                    catch
                    {
                        HasJoiningPredecessor = null;
                        Logger.Error($"Rejecting join for {request.Sender} because predecessor say it's expecting a successor");
                        throw new RejectedException("Predecessor busy, try again");
                    }

                    var myOldPredecessor = PredecessorNode;

                    try
                    {
                        var successorsList = MakeSuccessorsList();
                        successorsList.Insert(0, PredecessorNode);
                        // Notify the predecessor about the new joining node
                        await SendMessageAsync(PredecessorNode, new NotifyNewSuccessor(request.Sender, Node, successorsList));
                        // Successor has updated
                        var newSuccessorList = MakeSuccessorsList();
                        JoiningPredecessor = request.Sender;
                        _ = Task.Run(async () =>
                        {
                            var sender = request.Sender;
                            await Task.Delay(10000);
                            if (JoiningPredecessor == sender)
                            {
                                Logger.Warn("Clearing JoiningPredecessor after timeout");
                                JoiningPredecessor = null;
                            }
                        });
                        return new JoinNetworkResponse(Node, myOldPredecessor, newSuccessorList);

                    }
                    catch (Exception ex)
                    {
                        Logger.Debug($"Exception when notifying predecessor {PredecessorNode} about the new joining node {request.Sender}:\n{ex}");
                        throw new RejectedException("Unable to update predecessor node with new successor:\n{ex}");
                    }
                    finally
                    {
                        HasJoiningPredecessor = null;
                    }
                }
                finally
                {
                    await Task.Delay(500);
                    UpdateLock.Release();
                }
                */
            });

            /*
            NetworkAdapter.AddHandler(async (LeavingNetwork request) => 
            {
                Logger.Info($"Node {request.LeavingNode} is leaving");
                //return new AckResponse();

                if (!await UpdateLock.AcquireAsync(TimeSpan.FromSeconds(10)))
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
            */


            /*
            NetworkAdapter.AddHandler(async (NotifyNewSuccessor request) =>
            {

                if (!await UpdateLock.AcquireAsync(TimeSpan.FromSeconds(10)))
                {
                    Logger.Debug("Unable to aquire lock for notify new successor request");
                    throw new RejectedException("Busy, try again");
                }

                try
                {
                    if (HasJoiningSuccessor == null)
                    {
                        Logger.Error("Was not notified about a joining successor");
                        throw new RejectedException("We haven't been notified about a new successor joining");
                    }

                    if (request.SuccessorNode != SuccessorNode)
                    {
                        throw new RejectedException("We will only accept join notifications from our successor");
                    }

                    SuccessorNode = request.JoiningNode;

                    // Ensure this node goes straight into the finger table
                    FingerTableAdd(request.JoiningNode);

                    HasJoiningSuccessor = null;

                    return new AckResponse();
                }
                finally
                {
                    UpdateLock.Release();
                }
            });
            */

        }

        public async Task<Node> FindSuccessor(ulong key)
        {
            int hopCount = 0;
            if (Util.Inside(key, Node.Hash + 1, SuccessorNode.Hash))
            {
                return new Node(SuccessorNode, hopCount);
            }
            var (predecessor, successor) = FingerTableQuery(key);
            while (true)
            {
                hopCount++;
                if (hopCount > 20)
                {
                    // Introduce random delay because it seems the finger table needs to update somewhere
                    await Task.Delay(Util.RandomInt(100, 900));
                }
                var result = await SendMessageAsync(predecessor, new FindSuccessor(key));
                if (!result.IsRedirect)
                {
                    if (hopCount > 50)
                    {
                        Logger.Warn($"Very high hop count {hopCount}");
                    }
                    return new Node(result.Node, hopCount);
                }
                predecessor = result.Node;
                if (hopCount > 100)
                {
                    throw new Exception("Reached 100 hops");
                }
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
                    throw new Exception("INFINITE LOOP");
                }
            }
        }

        /// <summary>
        /// Find the nearest successor including key (searching for key 123 can return node 123)
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        /*
        public async Task<Node> QuerySuccessor(ulong key)
        {
            if (Util.Inside(key, PredecessorNode.Hash + 1, Node.Hash))
            {
                // Key belongs with this node
                return Node;
            }

            var (predecessor, successor) = FingerTableQuery(key);

            int hopCount = 0;
            while (true)
            {
                hopCount++;
                var result = await SendMessageAsync(predecessor, new FindPredecessor(key));

                if (result.IsRedirect)
                {
                    predecessor = result.Node;
                }
                else
                {
                    return new Node(result.Node, hopCount);
                }
            }
        }
        
        public Task<Node> QuerySuccessor(string key) => QuerySuccessor(Hash(key));
        public Task<Node> QuerySuccessor(Node node) => QuerySuccessor(node.Hash);
        */
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
                FindSuccessorReply joinLookup;
                while (true)
                {
                    joinLookup = await SendMessageAsync(nodeToJoin, new FindSuccessor(Node.Hash));
                    if (joinLookup.IsRedirect)
                    {
                        nodeToJoin = joinLookup.Node;
                    }
                    else
                    {
                        break;
                    }
                }
                var joiningTo = joinLookup.Node;
                if (joiningTo == Node)
                {
                    // If QuerySuccessor returns the current node, it means the node to join was down and QuerySuccessor fell back to the successor
                    Logger.Warn($" - node {nodeToJoin} seems to be down");
                    throw new RejectedException($"Unable to join via node {nodeToJoin}, node down?");
                }

                try
                {
                    var joinRequested = await SendMessageAsync(joiningTo, new JoinNetwork());

                    PredecessorNode = joinRequested.PredecessorNode;
                    SuccessorNode = joinRequested.SuccessorNode;
                    SetSuccessorsList(joinRequested.Successors);
                    Logger.Ok("Joining node is rebuilding the finger table");
                    await BuildFingerTable();
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
            catch (Exception)
            {
                // Don't care if this fails, we'll let the network clean up the mess after we're gone
            }

            await ResetState();
        }

        public async Task ResetState()
        {

            await NeighborManager.Serial(() =>
            {
                PredecessorNode = Node;
                PredecessorFailed = false;
                SuccessorNode = Node;
                SuccessorFailed = false;
                SuccessorsList = new List<Node>();
                LastFingerTableChange = DateTime.UtcNow;

                // Clear our finger table
                for (int i = 0; i < FingerCount; i++)
                {
                    Finger[i] = Node;
                }

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

            try
            {
                var predecessor = await SendMessageAsync(PredecessorNode, new RequestNodeInfo());
                if (predecessor.SuccessorNode != Node)
                {
                    Logger.Warn($"Predecessor reports having a different successor {predecessor.SuccessorNode} than me");
                    PredecessorFailed = true;
                }
                else
                {
                    PredecessorFailed = false;
                }
            }
            catch (Exception ex)
            {
                Logger.Warn($"Predecessor did not respond:\n{ex}");
                PredecessorFailed = true;
            }

            try
            {
                var successor = await SendMessageAsync(SuccessorNode, new RequestNodeInfo());
                if (successor.PredecessorNode != Node)
                {
                    Logger.Warn($"Successor reports having a different predecessor {successor.PredecessorNode} than me");
                    Logger.Error("SUCCESSOR CRAP");
                    SuccessorFailed = true;
                }
                else
                {
                    SuccessorFailed = false;
                }
            }
            catch (Exception ex)
            {
                Logger.Error("SUCCESSOR CRAP");
                Logger.Warn($"Successor did not respond:\n{ex}");
                SuccessorFailed = true;
            }

            if (SuccessorFailed)
            {
                Logger.Error($"SUCCESSOR FAILED {SuccessorFailedCounter++} times");
            }

            await BuildFingerTable();
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
                            var nodeInfo = await SendMessageAsync(Finger[i], new RequestNodeInfo());
                            if (!Util.Inside(nodeInfo.PredecessorNode.Hash, start, Node.Hash))
                            {
                                // We have the correct finger
                                node = Finger[i];
                                // Caching long because we are certain
                                FingerCache.Set(start, node, TimeSpan.FromMilliseconds(Util.RandomInt(3000, 10000)));
                            }
                            else if (Util.RandomInt(0, 100) < 90)
                            {
                                // Node at the finger has a better predecessor so we use most of the time
                                node = nodeInfo.PredecessorNode;
                                // Caching short because we might not have found the best finger node yet
                                //FingerCache.Set(start, node, TimeSpan.FromMilliseconds(Util.RandomInt(500, 2000)));
                            }
                            else
                            {
                                node = await FindSuccessor(start);
                                // Caching long because we are certain
                                FingerCache.Set(start, node, TimeSpan.FromMilliseconds(Util.RandomInt(3000, 10000)));
                            }
                        }
                        catch
                        {
                            Logger.Warn($"Used FindSuccessor to update finger table");
                            node = await FindSuccessor(start);
                            // Caching long because we are certain
                            FingerCache.Set(start, node, TimeSpan.FromMilliseconds(Util.RandomInt(3000, 10000)));
                        }

                    }
                    
                }
                // Logger.Debug($"Finger {i} {Util.Percent(start)}: {node}");
                if (PredecessorNode == Node)
                {
                    // Stop updating finger table if we leave the network during finger table rebuild
                    return;
                }
                Finger[i] = node;
            }
            return;
        }
        /*
        public async Task ComplexCrapBuildFingerTable(int maxNodesToUpdate = 2)
        {
            bool didChange = false;
            var firstFinger = NextFingerToRebuild;
            Node? previousNode = null;
            Node? node;
            while (maxNodesToUpdate-- > 0)
            {
                var start = Node.Hash + Util.Pow(2, (ulong)NextFingerToRebuild);

                while (Util.Inside(node.Hash, start, Node.Hash))
                {
                    // The node.Hash is between start and Node.Hash inclusive, taking into account the circular keyspace
                    if (Finger[NextFingerToRebuild] != node)
                    {
                        didChange = true;
                    }
                    Finger[NextFingerToRebuild] = node;
                    NextFingerToRebuild = (NextFingerToRebuild + 1) % FingerCount;
                    if (NextFingerToRebuild == 0 || !Util.Inside(node.Hash, Node.Hash + Util.Pow(2, (ulong)NextFingerToRebuild), Node.Hash))
                    {
                        // Ensure that node is not used for earlier fingers or fingers that it shouldn't be used for
                        break;
                    }
                    start = Node.Hash + Util.Pow(2, (ulong)NextFingerToRebuild);
                }
                if (NextFingerToRebuild == firstFinger)
                {
                    // Never update any fingers twice
                    break;
                }
            }
            if (didChange)
            {
                Logger.Ok("Finger table updated");
                LastFingerTableChange = DateTime.UtcNow;
            }
            return;
        }
        */

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
            message.Receiver = to;

            var result = await NetworkAdapter.SendMessageAsync(message);
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
            if (nodeToDelete == SuccessorNode)
            {
                throw new Exception("Can't remove the successor node from the finger table");
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
              
                var existingNode = Finger[i].Hash - start;
                var candidateNode = nodeToAdd.Hash - start;
                if (candidateNode < existingNode && Finger[i] != Node)
                {
                    //Logger.Error($"FingerTableAdd: Finger {i} (start={Util.Percent(start)} {start}) Replacing {Finger[i]} with {nodeToAdd}");
                    newFingerTable[i] = nodeToAdd;
                    didChange = true;
                }
            }

            lock (Finger)
            {
                Finger = newFingerTable;
            }

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
        
        private void SetSuccessorsList(List<Node> successors)
        {
            if (successors.Count == 0)
            {
                throw new Exception("Will not set an empty successors list");
            }
            List<Node> newSuccessors = new List<Node>();
            foreach (Node node in successors)
            {
                if (newSuccessors.Count == 0)
                {
                    if (node != SuccessorNode)
                    {
                        throw new Exception($"Expected first node {node} in successors list to be my successor {SuccessorNode}");
                    }
                    newSuccessors.Add(node);
                    continue;
                }
                if (node == Node)
                {
                    break;
                }
                if (node == PredecessorNode)
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

        private ulong RelativeDistance(Node node)
        {
            return node.Hash - Node.Hash;
        }

        private void LogState()
        {
            Logger.Debug($"STATE {Node} predecessor={PredecessorNode} successor={SuccessorNode}");
        }

        private NodeList GetNodeList()
        {
            NodeList list = new NodeList(HashFunction);
            if (!PredecessorFailed) list.Add(PredecessorNode);
            if (!SuccessorFailed) list.Add(SuccessorNode);
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


