using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace ChordDHT.Util
{
    internal class Responder
    {
        public static void RespondJson(HttpListenerContext context, object? data)
        {
            var response = context.Response;
            response.ContentType = "application/json";
            string json = JsonSerializer.Serialize(data);
            response.OutputStream.Write(Encoding.UTF8.GetBytes(json));
            response.OutputStream.Close();
        }
    }
}
