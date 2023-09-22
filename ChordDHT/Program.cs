using System.Net;
using ChordDHT.ChordProtocol;
using ChordDHT.Util;

class Program
{

    static Dictionary<string, string> map = new Dictionary<string, string>();
    static Chord? chord;

    static async Task Main(string[] args)
    {

        if (args.Length < 2)
        {
            Console.WriteLine("Arguments: <hostname> <startPort> <numberOfNodes>");
            return;
        }

        var hostname = args[1];
        var startPort = int.Parse(args[2]);
        var numberOfNodes = 3; // antall noder du vil opprette
        var tasks = new List<Task>();

        for (int i = 0; i < numberOfNodes; i++)
        {
            int port = startPort + i;
            tasks.Add(serve(hostname, port, numberOfNodes, startPort));
        }

        await Task.WhenAll(tasks);
        //if (args.Length == 0)
        //{
        //    Console.WriteLine("Arguments: chord serve <hostname> <port> [node_ip:node_port]");
        //    return;
        //}

        //switch (args[0])
        //{
        //    case "serve":
        //        var hostname = args[1];
        //        var port = int.Parse(args[2]);
        //        serve(hostname, port);
        //        break;
        //}
    }

    static ulong hash(string key)
    {
        return Chord.DefaultHashFunction(key);
    }

    static async Task serve(string hostname, int port, int numberOfNodes, int startPort)
    {
        Router router = new Router();
        router.addRoute(new Route("GET", @"^/storage/neighbors$", getStorageNeighbors));
        router.addRoute(new Route("GET", @"^/storage/(?<key>\w+)$", getStorageKey));

        chord = new Chord($"{hostname}:{port}");
        for (int i = 0; i < numberOfNodes; i++)
        {
            int otherPort = startPort + i;
            if (otherPort != port)
            {
                chord.addNode($"{hostname}:{otherPort}");
            }
        }


        HttpListener listener = new HttpListener();
        listener.Prefixes.Add($"http://localhost:{port}/");
        listener.Start();
        Console.WriteLine($"Chord DHT: Listening on port {port}...");

        try
        {
            while (true) 
            {
                HttpListenerContext context = await listener.GetContextAsync();
                await HandleContextAsync(context, router);
            }
        }
        catch( Exception e )
        {
            Console.WriteLine($"error occured: {e.Message}");
        }
            /*
            HttpListenerContext context = listener.GetContext();
            RequestVariables requestVariables = new RequestVariables();
            if (router.handleRequest(context, requestVariables))
            {
                continue;
            }

            notFoundHandler(context);
            */
    }

    static async Task HandleContextAsync(HttpListenerContext context, Router router)
    {
        RequestVariables requestVariables = new RequestVariables();
        if (await router.HandleRequestAsync(context, requestVariables)) 
        {
            return;
        }
        notFoundHandler(context);
    }



    /**
     * Returns HTTP code 200, with value, if <key> exists in the DHT.
     * Returns HTTP code 404, if <key> does not exist in the DHT.
     */
    static async Task<object?> getStorageKey(HttpListenerContext ctx, RequestVariables values)
    {
            if (chord == null)
            {
                // Handle null chord instance appropriately.
                // For example:
                ctx.Response.StatusCode = 500; // Internal Server Error
                ctx.Response.Close();
                return "Chord instance is null";
            }

            var node = chord.lookup(values["key"]);

            if (node == chord.nodeName)
            {
                // The key belongs to this node, so return it or null if we don't have it
                return map.ContainsKey(values["key"]) ? map[values["key"]] : null;
            } 
            else
            {
                // The key belongs to another node (probably the node named in the variable `node`)
                HttpClient client = new HttpClient();
                var response = await client.GetAsync($"http://{node}/storage/{values["key"]}");
                if (response != null)
                {
                    return response.Content.ReadAsStringAsync();
                }

            }

            var value = new 
                {
                    keyIsLocal = node == chord.nodeName,
                    node = node,
                    value = map.ContainsKey(values["key"]) ? map[values["key"]] : null
                };

            string responseString = "Hello, World!";
            byte[] buffer = System.Text.Encoding.UTF8.GetBytes(responseString);
            ctx.Response.ContentLength64 = buffer.Length;
            Stream output = ctx.Response.OutputStream;
            output.Write(buffer, 0, buffer.Length);
            output.Close();
            return true;
    }

        /**
         * Returns HTTP code 200, with list of neighbors, as JSON.
         */
        static object getStorageNeighbors(HttpListenerContext ctx)
        {
            return new List<string> { "Hei", "hopp" };
        }

        static bool notFoundHandler(HttpListenerContext ctx)
        {
            string responseString = "Not Found";
            byte[] buffer = System.Text.Encoding.UTF8.GetBytes(responseString);
            ctx.Response.StatusCode = 404;
            ctx.Response.ContentLength64 = buffer.Length;
            Stream output = ctx.Response.OutputStream;
            output.Write(buffer, 0, buffer.Length);
            output.Close();
            return true;
        }

    
}
