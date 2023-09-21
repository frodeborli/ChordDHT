using System;
using System.Collections.Generic;
using System.Linq;
using System.Numerics;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Linq;
using System.Numerics;
using System.Runtime.Intrinsics.Arm;

namespace ChordDHT.ChordProtocol
{
    public class Chord : IChord
    {
        // The current node name
        public string nodeName;

        // The largest hash stored at this node
        public ulong nodeId;

        // The smallest hash stored at this node
        public ulong start;

        protected int fingerCount;
        protected float baseFactor;
        protected string[] fingers;
        protected Func<string, ulong> hashFunction;
        protected List<string> knownNodes;
        protected string successorNode;
        protected string predecessorNode;

        public Chord(string nodeName, Func<string, ulong> hashFunction = null)
        {
            this.nodeName = nodeName;
            nodeId = (hashFunction ?? DefaultHashFunction)(nodeName);
            start = this.nodeId + 1;
            predecessorNode = nodeName;
            successorNode = nodeName;

            if (hashFunction == null)
            {
                this.hashFunction = DefaultHashFunction;
            } else
            {
                this.hashFunction = hashFunction;
            }

            this.fingerCount = (int)Math.Log2(ulong.MaxValue);
            this.fingers = new string[fingerCount];
            this.knownNodes = new List<string> { this.nodeName };
            this.updateFingersTable();
            for (int i = 0; i < fingers.Length; i++) {
                var finger = fingers[i];
                Console.WriteLine($"Finger {i} '{finger}'");
            }
        }

        public void addNode(string nodeName)
        {
            if (this.knownNodes.IndexOf(nodeName) >= 0)
            {
                // Ignore duplicate add
                return;
            }
            this.knownNodes.Add(nodeName);
            this.updateFingersTable();
        }

        public void removeNode(string nodeName)
        {
            if (this.knownNodes.IndexOf(nodeName) == -1)
            {
                // Ignore nodes that don't exist
                return;
            }
            this.knownNodes.Remove(nodeName);
            this.updateFingersTable();
        }

        protected void updateFingersTable()
        {
            // Ensure known nodes is sorted after their node position
            knownNodes.Sort((a, b) =>
            {
                var ah = hash(a);
                var bh = hash(b);
                if (ah < bh)
                    return -1;
                else if (ah > bh)
                    return 1;
                else
                    return 0;
            });

            // find my predecessor
            var predecessorIndex = wrap(knownNodes.IndexOf(this.nodeName) - 1, knownNodes.Count);
            predecessorNode = knownNodes[predecessorIndex];
            start = hash(predecessorNode) + 1;

            ulong fingerOffset = 1;
            int nextNodeToUpdate = 0;
            // Assign each node to the correct finger O(n^2) where n = number of fingers
            for (uint i = 0; i < fingers.Length; i++)
            {
                var startValue = this.nodeId + fingerOffset;
                var nextValue = startValue + fingerOffset - 1;
                fingerOffset *= 2;
                for (int j = 0; j < knownNodes.Count; j++)
                {
                    if (inside(hash(knownNodes[j]), startValue, nextValue))
                    {
                        // The node belongs to finger j, but we'll update all fingers from
                        // `nextNodeToUpdate` to `j`.
                        while (nextNodeToUpdate <= i)
                        {
                            if (nextNodeToUpdate == 0)
                            {
                                successorNode = knownNodes[j];
                            }
                            fingers[nextNodeToUpdate++] = knownNodes[j];
                        }
                        startValue = nextValue + 1;
                        break;
                    }
                }
            }
        }

        /**
         * Check if hash is between floor and ceiling inclusive, while
         * allowing wrapping around the key space size.
         */
        public bool inside(ulong hash, ulong floor, ulong ceiling)
        {
            if (ceiling >= floor)
            {
                return (hash >= floor) && (hash <= ceiling);
            } else
            {
                return hash >= ceiling || hash <= floor;
            }
        }

        public bool isReady()
        {
            throw new NotImplementedException();
        }

        /**
         * Given a key, find which node is the best node to answer the question
         */
        public string lookup(string key)
        {
            ulong keyHash = hash(key);
            ulong distance;

            // Check if this key belongs to me
            if (inside(keyHash, this.start, this.nodeId))
            {
                return this.nodeName;
            }

            // Handle wrapping of node ids
            if (keyHash > this.nodeId)
            {
                distance = keyHash - this.nodeId;
            }
            else
            {
                distance = (ulong.MaxValue - this.nodeId) + keyHash + 1;
            }

            int fingerIndex = (int)Math.Floor(Math.Log2(distance));
            return fingers[Math.Min(fingerIndex, fingers.Length - 1)];
        }

        protected ulong hash(string key)
        {
            return (this.hashFunction)(key);
        }

        protected int wrap(int value, int max)
        {
            return ((value % max) + max) % max;
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
