using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace ChordDHT.Util
{
    internal class Debug
    {
        public static void Dump(HttpListenerResponse response)
        {
            Console.WriteLine("---- HttpListenerResponse Debug Info ----");

            // Status code and description
            Console.WriteLine($"Status Code: {response.StatusCode}");
            Console.WriteLine($"Status Description: {response.StatusDescription}");

            // Headers
            Console.WriteLine("Headers:");
            foreach (string key in response.Headers.Keys)
            {
                string value = response.Headers[key];
                Console.WriteLine($"  {key}: {value}");
            }

            // Other properties
            Console.WriteLine($"Content Length64: {response.ContentLength64}");
            Console.WriteLine($"Protocol Version: {response.ProtocolVersion}");
            Console.WriteLine($"Keep Alive: {response.KeepAlive}");
            Console.WriteLine($"Redirect Location: {response.RedirectLocation}");
            Console.WriteLine($"Send Chunked: {response.SendChunked}");

            Console.WriteLine("----------------------------------------");
        }
    }
}
