using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace ChordDHT.Util
{
    internal class Send
    {
        public static async Task JSON(HttpListenerContext context, object? data)
        {
            var response = context.Response;
            response.ContentType = "application/json";
            string json = JsonSerializer.Serialize(data);
            await response.OutputStream.WriteAsync(Encoding.UTF8.GetBytes(json));
            response.OutputStream.Close();
        }
    }
}
