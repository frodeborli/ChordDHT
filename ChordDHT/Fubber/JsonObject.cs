using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Fubber
{
    public class JsonObject : IDictionary<object, object>
    {
        private object _Data;
        private OrderedDictionary<string, object>? _Dict;

        public JsonObject(object data)
        {
            if (data == null)
            {
                throw new InvalidOperationException("Can't wrap null values");
            }
            _Data = data;

            if (data is List<object> list)
            {
                _List = 
                foreach (object item in list)
                {

                }
            }
            if (data is Dictionary<object, object> dict)
            {
                var _StringDict = new Dictionary<string, object>();
                foreach (object key in dict.Keys)
                {
                    _Dict.Add(DictionaryKey(key), dict[key]);
                }
            } else if (data is List<object> list)
            {
                var _IntDict = new Dictionary<int, object>();
                _Dict = _IntDict;
            }
        }

        public JsonObject? this[string fieldName]
        {
            get
            {
                if (_Data is IDictionary<string, object> dict)
                {
                    dict.TryGetValue(fieldName, out var value);
                    if (value == null)
                    {
                        return null;
                    }
                    return new JsonObject(value);
                }
                var property = _Data.GetType().GetProperty(fieldName);
                if (property == null)
                {
                    return null;
                }
                return new JsonObject(property.GetValue(_Data)!);
            }
        }

        public JsonObject? this[int index]
        {
            get
            {
                if (_Data is IList<object> list)
                {
                    if (index >= 0 && index < list.Count)
                    {
                        return new JsonObject(list[index]);
                    }
                }
                return null;
            }
        }

        // For int
        public static implicit operator int(JsonObject obj)
        {
            return Convert.ToInt32(obj._Data);
        }

        // For double
        public static implicit operator double(JsonObject obj)
        {
            return Convert.ToDouble(obj._Data);
        }

        // For float
        public static implicit operator float(JsonObject obj)
        {
            return Convert.ToSingle(obj._Data);
        }

        // For string
        public static implicit operator string?(JsonObject obj)
        {
            if (obj._Data is string str)
            {
                return str;
            }
            return Convert.ToString(obj._Data);
        }

        // For bool
        public static implicit operator bool(JsonObject obj)
        {
            return Convert.ToBoolean(obj._Data);
        }

        private static string DictionaryKey(object key)
        {
            return key?.ToString() ?? "";
        }
    }
}
