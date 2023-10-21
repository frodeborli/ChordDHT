using Fubber;
using System.Security.Cryptography;
using System.Text;

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

        public Func<byte[], ulong> HashFunction { get; private set; }

        // Using ulong hashes which is 64 bits, therefore the finger table is constant 64 bit
        public const int FingerCount = 64;

        // Maintains a red black tree of nodes sorted according to their hash value
        private NodeList _KnownNodes;

        // Network adapter is used for inter node communications and must be provided
        private IChordNetworkAdapter NetworkAdapter;


        public Chord(string nodeName, IChordNetworkAdapter networkAdapter, Func<byte[], ulong>? hashFunction = null)
        {
            Node = new Node(nodeName, Hash(nodeName));
            NetworkAdapter = networkAdapter;
            HashFunction = hashFunction ?? DefaultHashFunction;

            // When there is only a single node, the predecessor and successor is always the same node
            PredecessorNode = Node;
            SuccessorNode = Node;

            Fingers = new Node[FingerCount];
            _KnownNodes = new NodeList(HashFunction);
            UpdateFingersTable();
        }

        public ulong Hash(byte[] key) => HashFunction(key);
        public ulong Hash(string key) => HashFunction(Encoding.UTF8.GetBytes(key));

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
