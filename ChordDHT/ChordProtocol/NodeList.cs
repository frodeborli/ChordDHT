using Fubber;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ChordProtocol
{
    public class NodeList : SortedDictionary<string, Node>
    {
        private readonly Func<string, ulong> HashFunction;

        public NodeList(Func<byte[], ulong> hashFunction)
            : base(new NodeComparer(hashFunction))
        {
            HashFunction = (key) => hashFunction(Encoding.UTF8.GetBytes(key));
        }

        public void Add(Node node)
        {
            if (ContainsKey(node.Name))
                return;
            Add(node.Name, node);
        }

        public void Add(string name)
        {
            Add(new Node(name, HashFunction(name)));
        }

        public void Remove(Node node)
        {
            Remove(node.Name);
        }

        public Node? FindSuccessor(Node node) => FindSuccessor(node.Hash);
        public Node? FindSuccessor(ulong hash)
        {
            if (this.Count == 0)
            {
                return null;
            }
            else if (this.Count == 1)
            {
                return this.ElementAt(0).Value;
            }

            int min = 0, max = this.Count - 1;
            Node? successor = null;

            while (min <= max)
            {
                int mid = (min + max) / 2;
                var current = this.ElementAt(mid).Value;

                if (current.Hash == hash)
                {
                    min = mid + 1;
                }
                else if (current.Hash < hash)
                {
                    min = mid + 1;
                }
                else
                {
                    successor = current;
                    max = mid - 1;
                }
            }

            // Wrap-around case
            if (successor == null)
            {
                successor = this.ElementAt(0).Value;
            }

            return successor;
        }

        private class NodeComparer : IComparer<string>
        {
            private readonly Func<string, ulong> HashFunction;

            public NodeComparer(Func<byte[], ulong> hashFunction)
            {
                HashFunction = (key) => hashFunction(Encoding.UTF8.GetBytes(key));
            }

            public int Compare(string? x, string? y)
            {
                if (x == null || y == null) return 0;
                return HashFunction(x).CompareTo(HashFunction(y));
            }
        }

    }

}
