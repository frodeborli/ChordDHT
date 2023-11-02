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

        public DateTime LastFingerTableChange { get; private set; } = DateTime.UtcNow;

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
        /// Caches QuerySuccessor results for 1.5 seconds.
        /// </summary>
        private Cache<ulong, Node> QuerySuccessorCache = new Cache<ulong, Node>(1024, TimeSpan.FromSeconds(5));

        private Cache<Node, NetworkException> DownCache = new Cache<Node, NetworkException>(128, TimeSpan.FromMilliseconds(1500));

        private DateTime? HasJoiningPredecessor = null;
        private DateTime? HasJoiningSuccessor = null;
        private Node? JoiningPredecessor = null;
        private bool HasCompletedJoin = false;
        private DateTime? JoinLockedUntil = null;

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
            NetworkAdapter.AddHandler(async (JoinNetworkCompleted request) =>
            {
                if (JoiningPredecessor == null)
                {
                    Logger.Error("JoinNetworkCompleted without a joining predecessor");
                    throw new RejectedException("Nobody is joining");
                }

                if (!await UpdateLock.AcquireAsync(TimeSpan.FromSeconds(10)))
                {
                    Logger.Debug("Unable to aquire lock for join request");
                    throw new RejectedException("Busy, try again.");
                }

                if (JoiningPredecessor != request.Sender)
                {
                    throw new RejectedException("Incorrect joiner");
                }

                try
                {
                    Logger.Notice($"Completed join from {request.Sender}");

                    if (PredecessorNode == Node)
                    {
                        SuccessorNode = JoiningPredecessor;
                    }
                    PredecessorNode = JoiningPredecessor;
                    JoiningPredecessor = null;
                    HasCompletedJoin = true;
                    return new AckResponse();
                }
                finally
                {
                    UpdateLock.Release();
                }
            });

            NetworkAdapter.AddHandler(async (JoinNetwork request) => 
            {
                Logger.Info($"Node {request.Sender} is requesting to join the network");

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
            });

            NetworkAdapter.AddHandler(async (PrepareForSuccessor request) =>
            {
                if (!await UpdateLock.AcquireAsync(TimeSpan.FromSeconds(10)))
                {
                    throw new RejectedException("Busy, try again");
                }

                try
                {
                    if (HasJoiningPredecessor != null)
                    {
                        Logger.Notice("I already have a joining predecessor");
                        var elapsed = DateTime.UtcNow - HasJoiningPredecessor;
                        if (elapsed < TimeSpan.FromSeconds(30))
                        {
                            Logger.Warn("I have an unresolved joining predecessor, but it has been 10 seconds so I will ignore that...");
                            HasJoiningPredecessor = null;
                        }
                        else
                        {
                            Logger.Warn("I have an unresolved joining predecessor and it has been less than 10 seconds, so rejected PrepareForSuccessor");
                            throw new RejectedException($"Handling a joining successor since {elapsed}");
                        }
                    }
                    if (HasJoiningSuccessor != null)
                    {
                        Logger.Notice("I already have a joining successor");
                        var elapsed = DateTime.UtcNow - HasJoiningSuccessor;
                        if (elapsed < TimeSpan.FromSeconds(30))
                        {
                            Logger.Warn("I have an unresolved joining successor, but it has been 10 seconds so I will ignore that...");
                            HasJoiningSuccessor = null;
                        }
                        else
                        {
                            Logger.Warn("I have an unresolved joining successor and it has been less than 10 seconds, so rejected PrepareForSuccessor");
                            throw new RejectedException($"Handling a joining successor since {elapsed}");
                        }
                    }
                    HasJoiningSuccessor = DateTime.UtcNow;
                    return new AckResponse();
                }
                finally
                {
                    UpdateLock.Release();
                }
            });

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

            NetworkAdapter.AddHandler(async (RequestNodeInfo request) =>
            {
                if (!await UpdateLock.AcquireAsync(TimeSpan.FromSeconds(10)))
                {
                    Logger.Debug("Unable to aquire lock for notify new successor request");
                    throw new RejectedException("Busy, try again");
                }
                try
                {
                    return new NodeInfoResponse(PredecessorNode, SuccessorNode, MakeSuccessorsList(), Node.Hash, Node.Name);
                }
                finally
                {
                    UpdateLock.Release();
                }

            });

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

            NetworkAdapter.AddHandler(async (RequestAsSuccessor request) =>
            {
                if (!await UpdateLock.AcquireAsync(TimeSpan.FromSeconds(10)))
                {
                    Logger.Debug("Unable to aquire lock for request as successor request");
                    throw new RejectedException("Busy, try again");
                }

                try
                {
                    if (PredecessorFailed)
                    {
                        Logger.Info($"Accepting a new predecessor {request.Sender} since our current predecessor {PredecessorNode} is failed");
                        PredecessorNode = request.Sender;
                        PredecessorFailed = false;
                        return new MakeSuccessorResponse(MakeSuccessorsList());
                    }
                    else
                    {
                        Logger.Notice($"Rejecting new predecessor {request.Sender} since our current predecessor {PredecessorNode} is not marked as failed");
                        return new MakeSuccessorResponse(PredecessorNode);
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
        public async Task<Node> QuerySuccessor(ulong key, Node? viaNode = default, bool autoRedirect = true, int _maxRetries = 5)
        {
            if (_maxRetries == 0)
            {
                throw new Exception("Max retries");
            }

            if (QuerySuccessorCache.TryGetValue(key, out var cachedNode, true))
            {
                Logger.Debug($"QuerySuccessor({key} found in QuerySuccessorCache");
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
                    QuerySuccessorCache.Clear();
                    if (!autoRedirect)
                        throw;
                    if (viaNode == targetNode)
                    {
                        return await QuerySuccessor(key, _maxRetries: _maxRetries - 1);
                    }
                    else
                    {
                        return await QuerySuccessor(key, viaNode, _maxRetries: _maxRetries - 1);
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

            QuerySuccessorCache.Clear();
            var joiningTo = await QuerySuccessor(Node.Hash, nodeToJoin);
            if (joiningTo == Node)
            {
                // If QuerySuccessor returns the current node, it means the node to join was down and QuerySuccessor fell back to the successor
                Logger.Warn($" - node {nodeToJoin} seems to be down");
                throw new RejectedException($"Unable to join via node {nodeToJoin}, node down?");
            }

            Logger.Info($" - Found successor {joiningTo} via {nodeToJoin}, requesting to join...");
            try
            {
                var joinRequested = await SendMessageAsync(joiningTo, new JoinNetwork());

                Logger.Info($" - Acknowledging join (predecessor={joinRequested.PredecessorNode} successor={joinRequested.SuccessorNode})");
                await SendMessageAsync(joiningTo, new JoinNetworkCompleted());
                PredecessorNode = joinRequested.PredecessorNode;
                SuccessorNode = joinRequested.SuccessorNode;
                SetSuccessorsList(joinRequested.Successors);
                HasCompletedJoin = true;
                await BuildFingerTable();


                Logger.Notice($" - Successfully joined network via node {joiningTo}");
                return;
            }
            catch (Exception ex)
            {
                QuerySuccessorCache.Clear();
                Logger.Warn($" - Unable to join at successor {joiningTo}");
                throw new RejectedException($"Unable to join at successor {joiningTo}\n{ex}");
            }

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

            ResetState();
        }

        public void ResetState()
        {
            lock (Finger)
            {
                HasCompletedJoin = false;
                PredecessorNode = Node;
                PredecessorFailed = false;
                SuccessorNode = Node;
                SuccessorFailed = false;
                SuccessorsList = new List<Node>();
                LastFingerTableChange = DateTime.UtcNow;
                QuerySuccessorCache.Clear();
                DownCache.Clear();

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
        public void Detach() => ResetState();
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
                /*
                if (SuccessorsList.Count > 0)
                {
                    List<Node> impossibleRecoveries = new List<Node>();
                    foreach (var recoverNode in SuccessorsList)
                    {
                        try {
                            await JoinNetwork(recoverNode);
                        }
                        catch (Exception er)
                        {
                            Logger.Warn($"Tried rejoining via node {recoverNode} but got:\n{er}");
                            impossibleRecoveries.Add(recoverNode);
                        }
                        await Task.Delay(500);
                    }
                    foreach (var recoverNode in impossibleRecoveries)
                    {
                        SuccessorsList.Remove(recoverNode);
                    }
                }
                */

                // Not networked, so no stabilization
                return;
            }
            if (!HasCompletedJoin)
            {
                // Logger.Debug("Has not completed join");
                return;
            }
            if (HasJoiningSuccessor != null || HasJoiningPredecessor != null)
            {
                // Logger.Debug("Skipping stabilization while handling a join");
                return;
            }

            Logger.Debug("Stabilization");

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
                    if (predecessor.SuccessorNode != Node)
                    {
                        if (predecessor.SuccessorNode == PredecessorNode)
                        {
                            Logger.Notice($"Predecessor {PredecessorNode} (with successor={predecessor.SuccessorNode}) disagrees about the relationship, marking as failed (did it just join?)");
                        }
                        else
                        {
                            Logger.Warn($"Predecessor {PredecessorNode} (with successor={predecessor.SuccessorNode}) disagrees about the relationship, marking as failed");
                        }
                        throw new NetworkException(predecessor.Sender, "Predecessor disagrees about the relationship");
                    }
                    if (PredecessorFailed)
                    {
                        Logger.Notice($"Predecessor {PredecessorNode} has recovered");
                    }
                }
                PredecessorFailed = false;
            }
            catch (RejectedException)
            {

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
                    if (successor.PredecessorNode != Node)
                    {
                        if (successor.PredecessorNode== SuccessorNode)
                        {
                            Logger.Notice($"Successor {SuccessorNode} (with predecessor={successor.PredecessorNode}) disagrees about the relationship, marking as failed (did it just join?)");
                        }
                        else
                        {
                            Logger.Warn($"Successor {SuccessorNode} (with predecessor={successor.PredecessorNode}) disagrees about the relationship, marking as failed");
                        }
                        throw new NetworkException(SuccessorNode, "Successor disagrees about the relationship");
                    }
                    if (SuccessorFailed)
                    {
                        Logger.Notice($"Successor {SuccessorNode} has recovered");
                    }

                    SetSuccessorsList(successor.Successors);
                }
                SuccessorFailed = false;
            }
            catch (RejectedException)
            {

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
                            MakeSuccessorResponse response;

                            bool success;
                            Node altSuccessor = backupSuccessor;
                            while (true)
                            {
                                Logger.Notice($"Trying successor {altSuccessor}");
                                var result = await TryRequestAsSuccessor(altSuccessor);
                                if (result != null)
                                {
                                    altSuccessor = result;
                                }
                                else
                                {
                                    break;
                                }
                            }
                            throw new RejectedException("Successor didn't want us");

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

        private async Task<Node?> TryRequestAsSuccessor(Node successor)
        {
            var response = await SendMessageAsync(successor, new RequestAsSuccessor(SuccessorNode));
            if (response.Successors != null)
            {
                Logger.Notice($"Alternative successor {successor} will replace previous successor {SuccessorNode}");
                SuccessorNode = successor;
                SuccessorFailed = false;
                SetSuccessorsList(response.Successors);
                SuccessorsList.Remove(SuccessorNode);
                return null;
            }
            return response.CurrentPredecessor;
        }

        /// <summary>
        /// Builds the finger table by only querying other nodes and not relying on own data
        /// </summary>
        /// <param name="successor">The node to start querying from (defaults to SuccessorNode)</param>
        /// <returns></returns>
        public async Task BuildFingerTable()
        {

            var list = GetNodeList();
            var newFingerTable = new Node[FingerCount];
            bool changed = false;

            for (ulong i = 0; i < FingerCount; i++)
            {
                ulong start = Node.Hash + Util.Pow(2, i);
                newFingerTable[i] = list.FindSuccessor(start)!;
                if (newFingerTable[i] != Finger[i])
                {
                    changed = true;
                }
            }
            if (changed)
            {
                LastFingerTableChange = DateTime.UtcNow;
                Logger.Warn("Updated finger table");
            }
            lock (Finger)
            {
                Finger = newFingerTable;
            }
            return;
            /*

            Node currentSuccessor = SuccessorNode;

            if (SuccessorFailed)
            {
                if (SuccessorsList.Count > 1)
                {
                    currentSuccessor = SuccessorsList[1];
                } 
                else
                {
                    Logger.Warn($"Unable to rebuild finger table when the successor node is marked as failed and no failover successor is available");
                }

            }

            Node[] newFingerTable = (Node[])Finger.Clone();
            var lastFingerTableChange = LastFingerTableChange;

            for (int i = 0; i < FingerCount; i++)
            {
                // The start offset for the finger
                ulong start = Node.Hash + Util.Pow(2, (ulong)i);

                if (!Util.Inside(currentSuccessor.Hash, start, Node.Hash))
                {
                    try
                    {
                        var newSuccessor = await QuerySuccessor(start);
                        currentSuccessor = newSuccessor;
                        Logger.Debug($"BuildFingerTable found successor {currentSuccessor} via QuerySuccessor({Util.Percent(start)}%)");
                    }
                    catch
                    {
                        // stop updating
                        break;
                    }
                }

                // currentSuccessor is valid for this finger
                if (newFingerTable[i] != currentSuccessor)
                {
                    LastFingerTableChange = DateTime.UtcNow;
                    Logger.Warn($"BuildFingerTable 0: Finger {i} (start={Util.Percent(start)} {start}) Replacing {newFingerTable[i]} with {currentSuccessor}");
                }
                newFingerTable[i] = currentSuccessor;
            }

            lock (Finger)
            {
                Finger = newFingerTable;
            }

            if (LastFingerTableChange != lastFingerTableChange)
            {
                Logger.Warn("Updated finger table");
            }
            */
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
            if (DownCache.TryGetValue(to, out var error))
            {
                //Logger.Error($"Reusing cached error {error}");
                throw new NetworkException(error.Node, error.Message);
            }

            message.Sender = Node;
            message.Receiver = to;

            try
            {
                var result = await NetworkAdapter.SendMessageAsync(message);
                FingerTableAdd(to);
                return message.Filter(result);
            }
            catch (NetworkException er)
            {
                QuerySuccessorCache.Clear();
                DownCache[to] = er;
                throw;
            }
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
                Logger.Error($"Tried to remove the successor node from the finger table");
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

        private ulong RelativeDistance(Node node)
        {
            return node.Hash - Node.Hash;
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


