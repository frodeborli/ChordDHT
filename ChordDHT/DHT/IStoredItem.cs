using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ChordDHT.DHT
{
    public interface IStoredItem
    {
        public string ContentType { get; }
        public byte[] Data { get; }

        public DateTime CreatedDate { get; }
    }
}
