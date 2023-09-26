using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ChordDHT.DHT
{
    public class DictionaryStorageBackend : IStorageBackend
    {
        private ConcurrentDictionary<string, IStoredItem> _dictionary = new ConcurrentDictionary<string, IStoredItem>();

        public async Task<bool> Remove(string key)
        {
            IStoredItem removedItem;
            return _dictionary.TryRemove(key, out removedItem);
        }

        public async Task<IStoredItem?> Get(string key)
        {
            if (_dictionary.TryGetValue(key, out var value))
            {
                return value;
            }
            return null;
        }

        public async Task<bool> ContainsKey(string key)
        {
            return _dictionary.ContainsKey(key);
        }

        public async Task<bool> Put(string key, IStoredItem value)
        {
            _dictionary[key] = value;
            return true;
        }
    }
}
