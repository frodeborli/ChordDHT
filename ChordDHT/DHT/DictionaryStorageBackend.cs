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

        public ConcurrentDictionary<string, IStoredItem> GetDictionary()
        {
            return _dictionary;
        }

        public Task<bool> Remove(string key)
        {
            IStoredItem? removedItem;
            return Task.FromResult(_dictionary.TryRemove(key, out removedItem));
        }

        public Task<IStoredItem?> Get(string key)
        {
            if (_dictionary.TryGetValue(key, out IStoredItem? val))
            {
                return Task.FromResult(val)!;
            }
            return Task.FromResult((IStoredItem?)null);
        }

        public Task<bool> ContainsKey(string key)
        {
            return Task.FromResult(_dictionary.ContainsKey(key));
        }

        public Task<bool> Put(string key, IStoredItem value)
        {
            _dictionary[key] = value;            
            return Task.FromResult(true);
        }
    }
}
