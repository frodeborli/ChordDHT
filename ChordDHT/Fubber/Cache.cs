using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ChordDHT.Fubber
{
    using System;
    using System.Collections;
    using System.Collections.Concurrent;
    using System.Collections.Generic;

    public class Cache<TKey, TValue> : IEnumerable<KeyValuePair<TKey, TValue>>, IEnumerable
        where TKey : notnull
    {
        private readonly int capacity;
        private readonly TimeSpan? maxTTL = null;
        private readonly ConcurrentDictionary<TKey, LinkedListNode<CacheItem>> cache;
        private readonly LinkedList<CacheItem> lruList;

        public Cache(int capacity, TimeSpan? maxTTL = null)
        {
            this.capacity = capacity;
            this.maxTTL = maxTTL;
            this.cache = new ConcurrentDictionary<TKey, LinkedListNode<CacheItem>>();
            this.lruList = new LinkedList<CacheItem>();
        }

        public TValue? this[TKey key]
        {
            get => Get(key);
            set => Set(key, value);
        }

        public TValue? Get(TKey key)
        {
            if (TryGetValue(key, out var value))
            {
                return value;
            }
            return default;
        }

        public bool Remove(TKey key, TValue? compareValue = default)
        {
            return TryRemove(key, out var node, compareValue);
        }

        public void Set(TKey key, TValue? value, TimeSpan expires) => Set(key, value, DateTime.UtcNow + expires);
        public void Set(TKey key, TValue? value, DateTime? expires = null)
        {
            if (maxTTL != null)
            {
                var maxExpires = DateTime.UtcNow + maxTTL;
                if (expires == null || maxExpires < expires)
                {
                    expires = maxExpires;
                }
            }
            TryRemove(key, out var node);
            if (value == null)
            {
                return;
            }
            MakeRoomForAtLeastOne();
            var newNode = new LinkedListNode<CacheItem>(new CacheItem(key, value, expires));
            lock (lruList)
            {
                lruList.AddLast(newNode);
                cache[key] = newNode;
            }
        }

        public void Clear()
        {
            lock (lruList)
            {
                lruList.Clear();
                cache.Clear();
            }
        }

        public bool ContainsKey(TKey key, bool renew = false) => TryGetValue(key, out TValue? value, renew);

        public bool TryGetValue(TKey key, out TValue? value, bool renew = false)
        {
            value = default;
            if (cache.TryGetValue(key, out var node))
            {
                if (!IsValidNode(node))
                {
                    Remove(key, node.Value.Value);
                    return false;
                }
                if (renew)
                {
                    lock (lruList)
                    {
                        lruList.Remove(node);
                        lruList.AddLast(node);
                    }
                }
                value = node.Value.KeyValuePair.Value;
                return true;
            }
            return false;
        }

        public bool TryRemove(TKey key, out TValue? value, TValue? compareValue = default)
        {
            value = default;
            lock (lruList)
            {
                if (cache.TryRemove(key, out var node))
                {
                    if (object.ReferenceEquals(compareValue, node.Value.Value))
                    {
                        cache[key] = node;
                        return false;
                    }
                    lruList.Remove(node);
                    if (!IsValidNode(node)) return false;
                    value = node.Value.KeyValuePair.Value;
                    return true;
                }
            }
            return false;
        }

        private bool IsValidNode(CacheItem node) => node.Expires == null || node.Expires >= DateTime.UtcNow;

        private bool IsValidNode(LinkedListNode<CacheItem> node) => IsValidNode(node.Value);

        private void MakeRoomForAtLeastOne(bool forceEvictionOfExpired = false)
        {
            if (!forceEvictionOfExpired && cache.Count < this.capacity - 1)
            {
                // There is room for at least one so we don't need to evict any
                return;
            }
            LinkedList<KeyValuePair<TKey, LinkedListNode<CacheItem>>> keysToEvict = new LinkedList<KeyValuePair<TKey, LinkedListNode<CacheItem>>>();
            lock (lruList)
            {
                foreach (var kvp in cache)
                {
                    if (!IsValidNode(kvp.Value))
                    {
                        keysToEvict.AddLast(kvp);
                    }
                }
                foreach (var node in keysToEvict)
                {
                    if (cache.TryRemove(node.Key, out var evictedNode))
                    {
                        lruList.Remove(evictedNode);
                    }
                }
            }
            if (cache.Count >= capacity)
            {
                lock (lruList)
                {
                    var lastNode = lruList.First;

                    cache.TryRemove(lastNode.Value.Key, out _);
                    lruList.RemoveFirst();
                }
            }
        }


        public IEnumerator<KeyValuePair<TKey, TValue>> GetEnumerator()
        {
            foreach (var node in cache)
            {
                if (IsValidNode(node.Value))
                    yield return node.Value.Value.KeyValuePair;
            }
        }

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        private class CacheItem
        {
            public KeyValuePair<TKey, TValue> KeyValuePair;
            public readonly DateTime? Expires;

            public TKey Key { get => KeyValuePair.Key; }
            public TValue Value { get => KeyValuePair.Value; }

            public CacheItem(KeyValuePair<TKey, TValue> kvp, DateTime? expires = null)
            {
                KeyValuePair = kvp;
                Expires = expires;
            }

            public CacheItem(TKey key, TValue value, DateTime? expires = null) : this(new KeyValuePair<TKey, TValue>(key, value), expires) { }
        }
    }

}
