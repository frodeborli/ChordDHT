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
        public List<string> KnownNodes { get; protected set; }
        public string SuccessorNode { get; private set; }
        public string PredecessorNode { get; private set; }

        private Func<string, ulong> HashFunction;
        private int FingerCount;
        private bool FingerTableUpdateScheduled = false;

        public Chord(string nodeName, Func<string, ulong> hashFunction = null)
        {
            this.NodeName = nodeName;
            NodeId = (hashFunction ?? DefaultHashFunction)(nodeName);
            Start = this.NodeId + 1;
            PredecessorNode = nodeName;
            SuccessorNode = nodeName;

            if (hashFunction == null)
            {
                this.HashFunction = DefaultHashFunction;
            } else
            {
                this.HashFunction = hashFunction;
            }

            this.FingerCount = (int)Math.Log2(ulong.MaxValue);
            this.Fingers = new string[FingerCount];
            this.KnownNodes = new List<string> { this.NodeName };
            for (int i = 0; i < Fingers.Length; i++) {
                var finger = Fingers[i];
            }
            UpdateFingersTable();
        }

        /**
         * Add knowledge about the existence of a node. This node
         * might not be used in routing, depending on the hash value
         * of the node name.
         */
        public void AddNode(string nodeName)
        {
            if (this.KnownNodes.IndexOf(nodeName) >= 0)
            {
                // Ignore duplicate add
                return;
            }
            this.KnownNodes.Add(nodeName);
            UpdateFingersTable();
        }

        /**
         * Remove knowledge about the existence of a node. This 
         * node might not be used in routing, depending on the hash
         * value of the node name - but removing it ensures that the
         * node will not end up in the finger table in the future.
         */
        public void RemoveNode(string nodeName)
        {
            if (this.KnownNodes.IndexOf(nodeName) == -1)
            {
                // Ignore nodes that don't exist
                return;
            }
            this.KnownNodes.Remove(nodeName);
            UpdateFingersTable();
        }

        /**
         * Given a key, find which node is the best node to answer the question
         */
        public string Lookup(string key)
        {
            if (FingerTableUpdateScheduled)
            {
                // Immediately update the finger table
                UpdateFingersTableReal();
            }

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
         * Generate a hash from a string
         */
        public ulong Hash(string key)
        {
            return (this.HashFunction)(key);
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
            if (!FingerTableUpdateScheduled)
            {
                FingerTableUpdateScheduled = true;
                Task.Run(() =>
                {
                    try
                    {
                        UpdateFingersTableReal();
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"{ex.GetType()}: {ex.Message}");
                    }
                });
            }
        }

        /**
         * Actually update the fingers table.
         */
        private void UpdateFingersTableReal()
        {
            if (!FingerTableUpdateScheduled)
            {
                return;
            }
            FingerTableUpdateScheduled = false;

            // Ensure known nodes is sorted according to their node position
            KnownNodes.Sort((a, b) =>
            {
                var ah = Hash(a);
                var bh = Hash(b);
                if (ah < bh)
                    return -1;
                else if (ah > bh)
                    return 1;
                else
                    return 0;
            });

            // Update the predecessor node
            var predecessorIndex = Wrap(KnownNodes.IndexOf(this.NodeName) - 1, KnownNodes.Count);
            PredecessorNode = KnownNodes[predecessorIndex];
            Start = Hash(PredecessorNode) + 1;

            ulong fingerOffset = 1;
            // Assign each node to the correct finger O(n^2) where n = number of fingers
            for (uint i = 0; i < Fingers.Length; i++)
            {
                var startValue = this.NodeId + fingerOffset;
                var nextValue = startValue + fingerOffset - 1;
                fingerOffset *= 2;

                // Find the nearest node
                var distance = ulong.MaxValue;
                for (int j = 0; j < KnownNodes.Count; j++)
                {
                    var nodeHash = Hash(KnownNodes[j]);
                    var candidateDistance = nodeHash - startValue;
                    if (candidateDistance < distance)
                    {
                        distance = candidateDistance;
                        Fingers[i] = KnownNodes[j];
                    }
                }

            }

            // Update the successor node
            SuccessorNode = Fingers[0];
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
                return hash < ceiling || hash > floor;
            }
        }

        public static ulong DefaultHashFunction(string key)
        {
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
