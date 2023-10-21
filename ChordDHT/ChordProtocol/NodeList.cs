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

        /**
         * Returns the Node having the greatest Node.Hash value
         * less than hash.
         */
        public Node? FindPredecessor(ulong hash)
        {
            int min = 0, max = this.Count - 1;
            Node? predecessor = null;

            while (min <= max)
            {
                int mid = (min + max) / 2;
                var current = this.ElementAt(mid).Value;

                if (current.Hash == hash)
                {
                    return current;
                }

                if (current.Hash < hash)
                {
                    predecessor = current;
                    min = mid + 1;
                }
                else
                {
                    max = mid - 1;
                }
            }

            // Wrap-around case
            if (predecessor == null)
            {
                predecessor = this.ElementAt(max).Value;
            }

            return predecessor;
        }

        public Node? FindPredecessor(string nodeName) => FindPredecessor(HashFunction(nodeName));

        public Node? FindPredecessor(Node node) => FindPredecessor(node.Hash);

        public Node? FindSuccessor(ulong hash)
        {
            if (this.Count == 0)
            {
                return null;
            }

            int min = 0, max = this.Count - 1;
            Node? successor = null;

            while (min <= max)
            {
                int mid = (min + max) / 2;
                var current = this.ElementAt(mid).Value;

                if (current.Hash == hash)
                {
                    return current;
                }

                if (current.Hash < hash)
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
                successor = this.ElementAt(min % this.Count).Value;
            }

            return successor;
        }

        //public Node? FindSuccessor(string nodeName) => FindSuccessor(HashFunction(nodeName));

        //public Node? FindSuccessor(Node node) => FindSuccessor(node.Hash);


        private class NodeComparer : IComparer<string>
        {
            private readonly Func<string, ulong> HashFunction;

            public NodeComparer(Func<byte[], ulong> hashFunction)
            {
                HashFunction = (key) => hashFunction(Encoding.UTF8.GetBytes(key));
            }

            public int Compare(string? x, string? y)
            {
                return HashFunction(x).CompareTo(HashFunction(y));
            }
        }

    }

}
