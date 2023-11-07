using ChordProtocol;
using Fubber;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace ChordDHT.ChordProtocol
{
    public sealed class Util
    {
        private static Random? _Random = null;
        public static Random Random
        {
            get
            {
                if (_Random == null)
                {
                    _Random = new Random();
                }
                return _Random;
            }
        }

        /// <summary>
        /// An implementation of a Hash function suitable for use with the Chord implementation
        /// using the first 64 bits of the resulting hash value. Note that this implementation
        /// is tied to the endianness of the computer running the node.
        /// </summary>
        /// <param name="key">A byte array from which a hash should be computed</param>
        /// <returns></returns>
        /// <exception cref="ArgumentNullException"></exception>
        public static ulong Sha1Hash(byte[] key)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key), "Key cannot be null");
            }
            using (SHA1 sha1 = SHA1.Create())
            {
                byte[] hash = sha1.ComputeHash(key);
                return BitConverter.ToUInt64(hash, 0);
            }
        }
        public static object? FromTypedJSON(string json)
        {
            string typeName;
            //Match match = Regex.Match(json, "\"\\$type\":\"([^\"]+)\"");
            //if (match.Success)
            //{
                //typeName = match.Groups[1].Value;
            //}
            //else
            
            {
                JsonDocument? jsonDocument = null;
                try
                {
                    jsonDocument = JsonDocument.Parse(json);
                }
                catch (Exception ex)
                {
                    Dev.Error($"FAILED PARSING JSON STRING '{json}'");
                    throw ex;
                }
                Dictionary<string, JsonElement> jsonObject = new Dictionary<string, JsonElement>();

                foreach (JsonProperty property in jsonDocument.RootElement.EnumerateObject())
                {
                    jsonObject.Add(property.Name, property.Value.Clone());
                }

                if (!jsonObject.ContainsKey("$type"))
                {
                    throw new InvalidDataException("$type annotation missing from message");
                }

                typeName = jsonObject["$type"].GetString();
                jsonObject.Remove("$type");
            }

            if (typeName == null)
            {
                throw new InvalidDataException("$type annotation missing from message");
            }

            var type = Type.GetType(typeName);
            try
            {
                // var strippedJson = json.Replace($",\"$type\":\"{type}\"", "");
                // Console.WriteLine($"{json}\n{strippedJson}");
                return JsonSerializer.Deserialize(json, type ?? typeof(object));
            }
            catch (Exception ex)
            {
                Console.WriteLine($"--- JSON: {json}");
                throw;
            }
        }

        public static string ToTypedJSON<T>(T o)
        {
            var originalJson = JsonSerializer.Serialize(o, o.GetType());
            var jsonDocument = JsonDocument.Parse(originalJson);

            Dictionary<string, JsonElement> jsonObject = new Dictionary<string, JsonElement>();
            foreach (JsonProperty property in jsonDocument.RootElement.EnumerateObject())
            {
                jsonObject.Add(property.Name, property.Value.Clone());
            }

            using (var jsonDoc = JsonDocument.Parse($"{{\"$type\": \"{o.GetType().AssemblyQualifiedName}\"}}"))
            {
                jsonObject["$type"] = jsonDoc.RootElement.GetProperty("$type").Clone();
            }
            return JsonSerializer.Serialize(jsonObject);
        }

        public static int RandomInt(int min, int max)
        {
            return Random.Next(min, max + 1);
        }

        public static ulong Pow(ulong b, ulong exponent)
        {
            if (exponent == 0) return 1;
            ulong result = 1;
            for (ulong i = 0; i < exponent; i++)
            {
                result *= b;
            }
            return result;
        }

        public static string Percent(ulong b)
        {
            return $"{b / (ulong.MaxValue / 100)}";
        }

        /**
         * Check if hash is between floor and ceiling inclusive, while
         * allowing wrapping around the key space size.
         */
        public static bool Inside(ulong hash, ulong floor, ulong ceiling)
        {
            if (floor <= ceiling)
            {
                return hash >= floor && hash <= ceiling;
            }
            else
            {
                return hash <= ceiling || hash >= floor;
            }
        }

        public static bool Ordered(ulong floor, ulong hash, ulong ceiling)
        {
            if (floor == ceiling) return hash == floor;
            if (floor < ceiling) return floor <= hash && ceiling >= hash;
            return floor >= hash || ceiling <= hash;
        }

        public static bool Ordered(Node floor, Node inside, Node ceiling) => Ordered(floor.Hash, inside.Hash, ceiling.Hash);

        public static T WarnIfSlow<T>(long ms, string name, Func<T> func)
        {
            var sw = Stopwatch.StartNew();
            T result = func();
            sw.Stop();
            if (sw.ElapsedMilliseconds > ms)
            {
                Console.WriteLine($"{name} took {sw.ElapsedMilliseconds} ms");
            }
            return result;
        }

        public static void WarnIfSlow(long ms, string name, Action func)
        {
            var sw = Stopwatch.StartNew();
            func();
            sw.Stop();
            if (sw.ElapsedMilliseconds > ms)
            {
                Console.WriteLine($"{name} took {sw.ElapsedMilliseconds} ms");
            }
        }

        public class JsonUlongAsHex : JsonConverter<ulong>
        {
            public override ulong Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
            {
                string hexStr = reader.GetString();
                if (hexStr != null)
                {
                    return ulong.Parse(hexStr, System.Globalization.NumberStyles.HexNumber);
                }
                throw new JsonException("Value cannot be null or non-hex format.");
            }

            public override void Write(Utf8JsonWriter writer, ulong ulongValue, JsonSerializerOptions options)
            {
                writer.WriteStringValue(ulongValue.ToString("X"));
            }
        }

    }
}
