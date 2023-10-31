using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ChordDHT.Fubber
{
    using System;
    using System.Collections;
    using System.Collections.Generic;

    public abstract class BaseDictionary<TKey, TValue> : IDictionary<TKey, TValue>
    {
        /// <summary>
        /// Tries to get the value associated with the specified key.
        /// </summary>
        /// <param name="key">The key whose value to get.</param>
        /// <param name="value">When this method returns, contains the value associated with the specified key, if found; otherwise, the default value for the type.</param>
        /// <returns>true if the key was found; otherwise, false.</returns>
        public abstract bool TryGetValue(TKey key, out TValue value);

        /// <summary>
        /// Tries to set the value for the specified key.
        /// </summary>
        /// <param name="key">The key whose value to set.</param>
        /// <param name="value">The value to set for the specified key.</param>
        /// <returns>true if the value was set successfully; otherwise, false.</returns>
        public abstract bool TrySetValue(TKey key, TValue value);

        /// <summary>
        /// Tries to remove the value for the specified key.
        /// </summary>
        /// <param name="key">The key whose value to remove.</param>
        /// <param name="removedValue">When this method returns, contains the value that was removed, if found; otherwise, the default value for the type.</param>
        /// <returns>true if the key was found and the value was removed; otherwise, false.</returns>
        public abstract bool TryRemoveValue(TKey key, out TValue removedValue);

        /// <summary>
        /// Gets the keys of the dictionary.
        /// </summary>
        public abstract ICollection<TKey> Keys { get; }

        /// <summary>
        /// Gets the values of the dictionary.
        /// </summary>
        public abstract ICollection<TValue> Values { get; }

        /// <summary>
        /// Gets the number of elements contained in the dictionary.
        /// </summary>
        public abstract int Count { get; }

        /// <summary>
        /// Removes all elements from the dictionary.
        /// </summary>
        public abstract void Clear();

        /// <summary>
        /// Returns an enumerator that iterates through the dictionary.
        /// </summary>
        /// <returns>An enumerator for the dictionary.</returns>
        public abstract IEnumerator<KeyValuePair<TKey, TValue>> GetEnumerator();

        /// <summary>
        /// Copies the elements of the dictionary to an array, starting at a particular index.
        /// </summary>
        /// <param name="array">The destination array.</param>
        /// <param name="arrayIndex">The index in the destination array at which to begin copying.</param>
        public abstract void CopyTo(KeyValuePair<TKey, TValue>[] array, int arrayIndex);



        // Default implementations for other IDictionary<TKey, TValue> members
        public TValue this[TKey key]
        {
            get
            {
                if (TryGetValue(key, out var value))
                {
                    return value;
                }
                throw new KeyNotFoundException();
            }
            set
            {
                TrySetValue(key, value);
            }
        }

        public bool IsReadOnly
        {
            get { return false; }
        }

        public void Add(TKey key, TValue value)
        {
            if (!TrySetValue(key, value))
            {
                throw new ArgumentException("An item with the same key has already been added.");
            }
        }

        public bool ContainsKey(TKey key)
        {
            return TryGetValue(key, out _);
        }

        public bool Remove(TKey key)
        {
            return TryRemoveValue(key, out _);
        }

        public void Add(KeyValuePair<TKey, TValue> item)
        {
            Add(item.Key, item.Value);
        }

        public bool Contains(KeyValuePair<TKey, TValue> item)
        {
            if (TryGetValue(item.Key, out var value))
            {
                return EqualityComparer<TValue>.Default.Equals(value, item.Value);
            }
            return false;
        }

        public bool Remove(KeyValuePair<TKey, TValue> item)
        {
            if (Contains(item))
            {
                return TryRemoveValue(item.Key, out _);
            }
            return false;
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }
}
