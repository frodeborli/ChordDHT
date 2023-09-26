using System.Diagnostics.Metrics;
using System.Net;
using System.Net.Http.Headers;
using System.Text.Encodings.Web;
using ChordDHT.Benchmark;
using ChordDHT.ChordProtocol;
using ChordDHT.DHT;
using ChordDHT.Util;

class Program
{
    public static void Main(string[] args)
    {
        if (args.Length == 0) {
            Console.WriteLine("Usage:");
            Console.WriteLine("  `chord serve <hostname> <port> [nodehost:node_port ...]`");
            Console.WriteLine("     Start a chord node and provide a list of other nodes:");
            Console.WriteLine("  `chord multiserve <hostname> <start_port> <nodes_to_start>`");
            Console.WriteLine("     Start multiple nodes on a single server.");
            Console.WriteLine("     nodes_to_start is the total number of nodes, minimum 1.");
            Console.WriteLine("  `chord benchmark <hostname> <port> <nodes_to_start>");
            Console.WriteLine("     Start a number of nodes and run testing and measurements.");
            return;
        }

        switch (args[0])
        {
            case "multiserve":
                // Start multiple nodes
                Multiserve(args);
                break;

            case "serve":
                // Start a single node
                Serve(args);
                break;

            case "benchmark":
                // Start multiple nodes and run testing
                Task.Run(() => Benchmark(args));
                break;
        }

        Console.WriteLine("Press a key to terminate");
        Console.ReadKey();
    }

    static void Serve(string[] args)
    {
        var hostname = args[1];
        var port = int.Parse(args[2]);
        var nodeCount = args.Length - 3 + 1;
        var nodeList = new string[nodeCount];
        nodeList[0] = $"{hostname}:{port}";
        for (int i = 3; i < args.Length; i++)
        {
            nodeList[i - 2] = args[i];
        }
        RunWebServer(hostname, port, nodeList);

    }

    static async void Multiserve(string[] args)
    {
        var hostname = args[1];
        var port = int.Parse(args[2]);
        var nodeCount = int.Parse(args[3]);
        if (nodeCount < 1)
        {
            Console.WriteLine("Must run at least one node");
            return;
        }
        var nodeList = new string[nodeCount];
        nodeList[0] = $"{hostname}:{port}";
        for (int i = 1; i < nodeCount; i++)
        {
            nodeList[i] = $"{hostname}:{port + i}";
        }
        Task[] tasks = new Task[nodeCount];

        Console.WriteLine("Starting instances...");

        for (int i = 0; i < nodeCount; i++)
        {
            int portNumber = port + i;
            tasks[i] = Task.Run(() =>
            {
                try
                {
                    RunWebServer(hostname, portNumber, nodeList);
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"{ex.GetType()}: {ex.Message}");
                }
            });
        }
    }

    static async void Benchmark(string[] args)
    {
        var hostname = args[1];
        var port = int.Parse(args[2]);
        var nodeCount = int.Parse(args[3]);
        if (nodeCount < 1)
        {
            Console.WriteLine("Must run at least one node");
            return;
        }
        var nodeList = new string[nodeCount];
        nodeList[0] = $"{hostname}:{port}";
        for (int i = 1; i < nodeCount; i++)
        {
            nodeList[i] = $"{hostname}:{port + i}";
        }
        Task[] tasks = new Task[nodeCount];

        Console.WriteLine("Starting instances...");

        for (int i = 0; i < nodeCount; i++)
        {
            int portNumber = port + i;
            tasks[i] = Task.Run(() =>
            {
                try
                {
                    RunWebServer(hostname, portNumber, nodeList);
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"{ex.GetType()}: {ex.Message}");
                }
            });
        }
        Console.WriteLine("Waiting 1 second before starting benchmarks...");
        await Task.Delay(1000);
        HttpClient httpClient = new HttpClient();
        Random random = new Random();
        string[] keys = new string[1000];
        for (int i = 0; i < 1000; i++)
        {
            keys[i] = RandomStringGenerator.GenerateRandomString(random.Next(1, 30));
        }

        var randomKey = () =>
        {
            return keys[random.Next(keys.Length)];
        };
        var randomNodeUrl = () =>
        {
            return $"http://{nodeList[random.Next(nodeList.Length)]}/storage/{randomKey()}";
        };


        await new Benchmark("GET requests for random keys on random hosts", async () => {
            await httpClient.GetAsync(randomNodeUrl());
        }).Run(200, 20);

        await new Benchmark("PUT requests for random keys on random hosts", async () => {
            var requestBody = new StringContent("Lorem Ipsum Dolor Sit Amet");
            requestBody.Headers.ContentType = MediaTypeHeaderValue.Parse("text/plain");
            await httpClient.PutAsync(randomNodeUrl(), requestBody);
        }).Run(200, 20);

        Console.WriteLine("Instances stopped");
        Console.ReadKey();
    }

    static ulong hash(string key)
    {
        return Chord.DefaultHashFunction(key);
    }

    static void RunWebServer(string hostname, int port, string[] nodeList)
    {
        HttpListener listener = new HttpListener();
        listener.Prefixes.Add($"http://{hostname}:{port}/");
        try
        {
            listener.Start();
        }
        catch (HttpListenerException e)
        {
            Console.WriteLine($"Error trying to open '{hostname}:{port}'");
            Console.WriteLine(e.ToString());
            return;
        }
        string nodeName = $"{hostname}:{port}";

        Router router = new Router();
        DHTServer dhtServer = new DHTServer($"{hostname}:{port}", nodeList, new DictionaryStorageBackend(), router, "/storage/");

        Console.WriteLine($"Chord DHT: Listening on port {port}...");
        while (true)
        {
            HttpListenerContext context = listener.GetContext();
            // Console.WriteLine($"HTTP/{context.Request.ProtocolVersion} {context.Request.Headers["User-Agent"]} {context.Request.HttpMethod} http://{hostname}:{port}{context.Request.RawUrl}");
            HandleContext(context, router);
        }
    }

    static void HandleContext(HttpListenerContext context, Router router)
    {
        RequestVariables requestVariables = new RequestVariables();

        if (!router.HandleRequest(context, requestVariables))
        {
            NotFoundHandler(context);
        }
    }

    static bool NotFoundHandler(HttpListenerContext ctx)
    {
        string responseString = $"The URL {WebUtility.HtmlEncode(ctx.Request.Url?.ToString())} Was Not Found";
        byte[] buffer = System.Text.Encoding.UTF8.GetBytes(responseString);
        ctx.Response.StatusCode = 404;
        ctx.Response.ContentLength64 = buffer.Length;
        Stream output = ctx.Response.OutputStream;
        output.Write(buffer, 0, buffer.Length);
        output.Close();
        return true;
    }

}
