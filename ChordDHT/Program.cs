using System.Net;
using ChordDHT.ChordProtocol;
using ChordDHT.Util;

class Program
{

    static Dictionary<string, string> map = new Dictionary<string, string>();
    static Chord? chord;

    static void Main(string[] args)
    {
        if (args.Length == 0) {
            Console.WriteLine("Arguments: chord serve <port> [node_ip:node_port]");
            return;
        }

        switch (args[0])
        {
            case "serve":
                var port = int.Parse(args[1]);
                serve(port);
                break;
        }
    }
    
    static ulong hash(string key)
    {
        return Chord.DefaultHashFunction(key);
    }

    static void serve(int port)
    {
        Router router = new Router();

        router.addRoute(new Route("GET", @"^/storage/neighbors$", getStorageNeighbors));
        router.addRoute(new Route("GET", @"^/storage/(?<key>\w+)$", getStorageKey));


        HttpListener listener = new HttpListener();
        listener.Prefixes.Add($"http://localhost:{port}/");
        listener.Start();
        Console.WriteLine($"Chord DHT: Listening on port {port}...");
        while (true)
        {
            HttpListenerContext context = listener.GetContext();
            RequestVariables requestVariables = new RequestVariables();
            if (router.handleRequest(context, requestVariables))
            {
                continue;
            }

            notFoundHandler(context);
        }

        /**
         * Returns HTTP code 200, with value, if <key> exists in the DHT.
         * Returns HTTP code 404, if <key> does not exist in the DHT.
         */
        static object getStorageKey(HttpListenerContext ctx, RequestVariables values)
        {
            return values["key"];
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
}
