using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace ChordDHT.Util
{
    public class WebApp
    {
        public Router Router { get; private set; }
        private static WebApp? _Instance = null;

        public static WebApp Instance
        {
            get
            {
                if (_Instance == null)
                {
                    throw new InvalidOperationException("Responder has not been initialized");
                }
                return _Instance;
            }
        }

        public static void Initialize(Router Router)
        {
            _Instance = new WebApp(Router);
        }

        public WebApp(Router router)
        {
            Router = router;
        }

        public void RespondJson(HttpListenerContext context, object? data)
        {
            var response = context.Response;
            response.ContentType = "application/json";
            string json = JsonSerializer.Serialize(data);
            response.OutputStream.Write(Encoding.UTF8.GetBytes(json));
            response.OutputStream.Close();
        }
    }
}
