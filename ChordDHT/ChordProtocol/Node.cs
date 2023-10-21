using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ChordProtocol
{
    public class Node
    {
        public readonly string Name;
        public readonly ulong Hash;
        public DateTime LastSeen
        {
            get; private set;
        }

        public Node(string name, ulong hash)
        {
            Name = name;
            Hash = hash;
            Tag();
        }

        public void Tag()
        {
            LastSeen = DateTime.UtcNow;
        }

        public string ToString()
        {
            return $"[Node {Name}]";
        }

        public static bool operator ==(Node a, Node b)
        {
            return a.Name == b.Name;
        }

        public static bool operator !=(Node a, Node b)
        {
            return a.Name != b.Name;
        }

    }
}
