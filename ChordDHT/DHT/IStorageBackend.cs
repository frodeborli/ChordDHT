using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ChordDHT.DHT
{
    public interface IStorageBackend
    {
        Task<bool> ContainsKey(string key);
        Task<IStoredItem?> Get(string key);
        Task<bool> Put(string key, IStoredItem value);
        Task<bool> Remove(string key);
    }
}
