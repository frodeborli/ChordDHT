using System.Collections.Concurrent;
using System.Security.Cryptography;
using System.Text;

namespace ChordDHT.ChordProtocol
{
    public class Chord : IChord
    {
        // The current node name
        public string NodeName;

        // The largest hash stored at this node
        public ulong NodeId;

        // The smallest hash stored at this node
        public ulong Start;

        public string[] Fingers { get; private set; }
        private HashSet<string> _KnownNodes;
        public string[] KnownNodes
        {
            get
            {
                return _KnownNodes.ToArray();
            }
        }

        public string SuccessorNode { get; private set; }
        public string PredecessorNode { get; private set; }

        public Func<string, ulong> Hash { get; private set; }

        private int FingerCount;

        /**
         * Lock the fingers table
         */
        private object FingerLock;

        public Chord(string nodeName, Func<string, ulong>? hashFunction = null)
        {
            this.FingerLock = new object();
            Hash = hashFunction ?? DefaultHashFunction;

            this.NodeName = nodeName;
            NodeId = Hash(nodeName);
            Start = this.NodeId + 1;
            PredecessorNode = nodeName;
            SuccessorNode = nodeName;

            this.FingerCount = (int)Math.Log2(ulong.MaxValue);
            this.Fingers = new string[FingerCount];
            this._KnownNodes = new HashSet<string> { this.NodeName };
            UpdateFingersTable();
            Console.WriteLine($"Successor node: {SuccessorNode}");
        }

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
                    if (_KnownNodes.Add(node))
                    {
                        Console.WriteLine($"{NodeName}: Added node '{node}' to list of known nodes");
                        addedCount++;
                    }
                }
            }
            if (addedCount > 0)
            {
                UpdateFingersTable();
            }
        }

        /**
         * Add knowledge about the existence of a node. This node
         * might not be used in routing, depending on the hash value
         * of the node name.
         */
        public void AddNode(string nodeName)
        {
            AddNode(new string[] { nodeName });
        }

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
                    if (node == NodeName)
                    {
                        throw new InvalidOperationException($"Illegal to remove self from known nodes list");
                    }
                    if (_KnownNodes.Remove(node))
                    {
                        Console.WriteLine($"{NodeName}: Removed node '{node}' from list of known nodes");
                        removedCount++;
                    }
                }
            }
            if (removedCount > 0)
            {
                UpdateFingersTable();
            }
        }

        public void RemoveNode(string nodeName)
        {
            RemoveNode(new string[] { nodeName });
        }

        /**
         * Given a key, find which node is the best node to answer the question
         */
        public string Lookup(string key)
        {
            ulong keyHash = Hash(key);
            ulong distance;

            // Check if this key belongs to me
            if (Inside(keyHash, this.Start, this.NodeId))
            {
                return this.NodeName;
            }

            // Handle wrapping of node ids
            if (keyHash > this.NodeId)
            {
                distance = keyHash - this.NodeId;
            }
            else
            {
                distance = (ulong.MaxValue - this.NodeId) + keyHash + 1;
            }

            int fingerIndex = (int)Math.Floor(Math.Log2(distance));

            return Fingers[Math.Min(fingerIndex, Fingers.Length - 1)];
        }

        /**
         * Wrap a value between 0 and max
         */
        private int Wrap(int value, int max)
        {
            return ((value % max) + max) % max;
        }

        /**
         * Request an update of the fingers table. The fingers table
         * will be updated as soon as the thread pool is available.
         */
        private void UpdateFingersTable()
        {
            // Avoid problems with concurrent processes causing the fingers table to be updated
            lock (FingerLock)
            {
                var knownNodes = new List<string>(_KnownNodes.ToArray());

                // We need a sorted list of nodes, so we can easily find our predecessor
                knownNodes.Sort((a, b) =>
                {
                    var ah = Hash(a) - NodeId;
                    var bh = Hash(b) - NodeId;
                    if (ah < bh)
                        return -1;
                    else if (ah > bh)
                        return 1;
                    else
                        return 0;
                });

                Console.WriteLine($"{string.Join(",", knownNodes)}");

                // Update the predecessor node
                PredecessorNode = knownNodes[Wrap(knownNodes.IndexOf(this.NodeName) - 1, knownNodes.Count)];

                // Our start hash is the predecessor node hash + 1
                Start = Hash(PredecessorNode) + 1;

                int bestNodeIndex = 0;
                // Find the best node for each finger table entry
                for (int i = 0; i < Fingers.Length; i++)
                {
                    var fingerOffset = NodeId + Math.Pow(2, i);

                    string? bestNode = null;
                    ulong bestDistance = ulong.MaxValue;
                    for (int j = 0; j < knownNodes.Count; j++)
                    {
                        ulong candidateHash = Hash(knownNodes[j]);
                        ulong distance = candidateHash - bestDistance;
                        if (bestNode == null || distance < bestDistance)
                        {
                            bestNode = knownNodes[j];
                            bestDistance = distance;
                        }
                    }

                    Fingers[i] = bestNode;
                }

                if (Fingers[0] == null)
                {
                    Console.WriteLine("FOUND A NULL FINGER!");
                }

                // Update the successor node
                SuccessorNode = Fingers[0];
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
        public static ulong DefaultHashFunction(string key)
        {
            if ( key == null )
            {
                throw new ArgumentNullException(nameof(key), "Key cannot be null");
            }
            using (SHA1 sha1 = SHA1.Create())
            {
                byte[] bytes = Encoding.UTF8.GetBytes(key);
                byte[] hash = sha1.ComputeHash(bytes);

                // Convert the hash to an UInt64 (not using BitConverter.ToUInt64 because we want to maintain endianness accross architectures)
                return BitConverter.ToUInt64(hash, 0);
            }
        }
    }
}
