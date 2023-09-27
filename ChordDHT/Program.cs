using System.Collections.Concurrent;
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
        ThreadPool.SetMinThreads(16, 64);
        if (args.Length == 0) {
            Console.WriteLine("Usage:");
            Console.WriteLine("  `chord serve <hostname> <port> [nodehost:node_port ...]`");
            Console.WriteLine("     Start a chord node and provide a list of other nodes:");
            Console.WriteLine("  `chord multiserve <hostname> <start_port> <nodes_to_start>`");
            Console.WriteLine("     Start multiple nodes on a single server.");
            Console.WriteLine("     nodes_to_start is the total number of nodes, minimum 1.");
            Console.WriteLine("  `chord benchmark <hostname:port> [hostname:port ...]");
            Console.WriteLine("     Run benchmarks against a list of nodes.");
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

        Console.WriteLine("Running for 5 minutes at most");
        Task.Delay(300000).Wait();
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

    static void Multiserve(string[] args)
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
        var nodeCount = args.Length - 1;
        if (nodeCount < 1)
        {
            Console.WriteLine("Must benchmark at least one node");
            return;
        }
        int repetitions = 8000;
        int workerThreads = 2; // nodeCount;
        var nodeList = new string[nodeCount];
        for (int i = 0; i < nodeCount; i++)
        {
            nodeList[i] = args[i+1];
        }
        foreach (string n in nodeList)
        {
            Console.WriteLine($"Node {n}");
        }
        await Task.Delay(2000);

        HttpClient httpClient = new HttpClient();
        Random random = new Random();
        string[] keys = new string[10000];
        for (int i = 0; i < 10000; i++)
        {
            keys[i] = RandomStringGenerator.GenerateRandomString(random.Next(5, 30));
        }
        var getRandomString = () => RandomStringGenerator.GenerateRandomString(random.Next(1, 30));
        var getRandomKey = () =>
        {
            return keys[random.Next(keys.Length)];
        };
        var getRandomNode = () => nodeList[random.Next(nodeList.Length)];
        var randomNodeAndKey = () =>
        {
            return $"http://{getRandomNode()}/storage/{getRandomKey()}";
        };
        var randomNodeFixedKey = (string key) =>
        {
            return $"http://{getRandomNode()}/storage/{key}";
        };
        var randomKeyFixedNode = (string node) =>
        {
            return $"http://{node}/storage/{getRandomKey()}";
        };
        ConcurrentDictionary<int, int> hopCounters = new ConcurrentDictionary<int, int>();
        ConcurrentDictionary<string, int> eventCounters = new ConcurrentDictionary<string, int>();
        var countHops = (int hopCount) =>
        {
            hopCounters.AddOrUpdate(hopCount, (key) => 1, (key, oldValue) => oldValue + 1);
        };
        var countEvent = (string name) =>
        {
            eventCounters.AddOrUpdate(name, (key) => 1, (key, oldValue) => oldValue + 1);
        };
        var processHttpClientResponse = (HttpResponseMessage response) =>
        {
            int? hopCount = null;
            if (response.Headers.Contains("X-Chord-Hops"))
            {
                hopCount = int.Parse(response.Headers.GetValues("X-Chord-Hops").First());
                if (hopCount.HasValue)
                {
                    countHops(hopCount ?? 0);
                }
            }
            countEvent($"HTTP {(int) response.StatusCode} {response.ReasonPhrase}");
            if (hopCount.HasValue)
            {
                return $"{hopCount} hops";
            } else
            {
                return null;
            }
        };
        var dumpExtraStatistics = () =>
        {
            Console.WriteLine(" HOP COUNTS:");
            int max = -1;
            for (int i = 0; i < 20; i++)
            {
                if (hopCounters.ContainsKey(i))
                {
                    max = i + 1;
                }
            }
            for (int i = 0; i < max; i++)
            {
                var hopCount = hopCounters.ContainsKey(i) ? hopCounters[i] : 0;
                Console.WriteLine($"  {i,2} hops: {hopCount,4}");
            }
            if (eventCounters.Count > 0)
            {
                Console.WriteLine();
                Console.WriteLine(" OTHER COUNTERS:");
                foreach (var key in eventCounters.Keys)
                {
                    Console.WriteLine($"  {key + ":",20}{eventCounters[key]}");
                }
            }
            Console.WriteLine();
            hopCounters = new ConcurrentDictionary<int, int>();
            eventCounters = new ConcurrentDictionary<string, int>();
        };

        await new Benchmark("GET requests for random keys on random hosts", async () => {
            var response = await httpClient.GetAsync(randomNodeAndKey());
            return processHttpClientResponse(response);
        }, dumpExtraStatistics).Run(repetitions, workerThreads);

        await new Benchmark("PUT requests for random keys on random hosts", async () => {
            var requestBody = new StringContent("Lorem Ipsum Dolor Sit Amet");
            requestBody.Headers.ContentType = MediaTypeHeaderValue.Parse("text/plain");
            var response = await httpClient.PutAsync(randomNodeAndKey(), requestBody);
            return processHttpClientResponse(response);
        }, dumpExtraStatistics).Run(repetitions, workerThreads);

        await new Benchmark("GET requests for key 'hello' on random hosts", async () => {
            var response = await httpClient.GetAsync(randomNodeFixedKey("hello"));
            return processHttpClientResponse(response);
        }, dumpExtraStatistics).Run(repetitions, workerThreads);

        await new Benchmark("PUT requests for key 'hello' on random hosts", async () => {
            var requestBody = new StringContent("Lorem Ipsum Dolor Sit Amet");
            requestBody.Headers.ContentType = MediaTypeHeaderValue.Parse("text/plain");
            var response = await httpClient.PutAsync(randomNodeFixedKey("hello"), requestBody);
            return processHttpClientResponse(response);
        }, dumpExtraStatistics).Run(repetitions, workerThreads);

        await new Benchmark("GET requests for random key on first host", async () => {
            var response = await httpClient.GetAsync(randomKeyFixedNode(nodeList[0]));
            return processHttpClientResponse(response);
        }, dumpExtraStatistics).Run(repetitions, workerThreads);

        await new Benchmark("PUT requests for key 'hello' on random hosts", async () => {
            var requestBody = new StringContent("Lorem Ipsum Dolor Sit Amet");
            requestBody.Headers.ContentType = MediaTypeHeaderValue.Parse("text/plain");
            var response = await httpClient.PutAsync(randomKeyFixedNode(nodeList[0]), requestBody);
            return processHttpClientResponse(response);
        }, dumpExtraStatistics).Run(repetitions, workerThreads);
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
