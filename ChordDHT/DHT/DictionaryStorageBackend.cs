using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ChordDHT.DHT
{
    public class DictionaryStorageBackend : IStorageBackend
    {
        private Dictionary<string, IStoredItem> _dictionary = new Dictionary<string, IStoredItem>();

        public async Task<bool> Remove(string key)
        {
            _dictionary.Remove(key);
            return true;
        }

        public async Task<IStoredItem?> Get(string key)
        {
            if (!_dictionary.ContainsKey(key))
            {
                return null;
            }
            return _dictionary[key];
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
