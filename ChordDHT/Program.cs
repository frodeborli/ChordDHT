using System.Collections.Concurrent;
using System.Diagnostics;
using System.Net.Http.Headers;
using ChordDHT.Benchmark;
using ChordDHT.ChordProtocol;
using ChordDHT.DHT;
using ChordDHT.Fubber;
using ChordProtocol;
using Fubber;

class Program
{
    public static void Main(string[] args)
    {
        var logger = Dev.Logger($"Main({string.Join(" ", args)})");
        int workerThreads;
        int completionPortThreads;
        ThreadPool.GetMaxThreads(out workerThreads, out completionPortThreads);
        ThreadPool.SetMinThreads(workerThreads, completionPortThreads);

        if (args.Length == 0)
        {
            Console.WriteLine("Usage:");
            Console.WriteLine("  `chord serve <hostname> <port> <nodename> [existing_node:port]`");
            Console.WriteLine("     Start a chord node and optionally join an existing network (by specifying an existing_node and port).\n");

            Console.WriteLine("  `chord testserve <hostname> <port> <count>");
            Console.WriteLine("     Start a multiple chord nodes without joining.\n");

            Console.WriteLine("  `chord multiserve <hostname> <start_port> <nodes_to_start>`\n");
            Console.WriteLine("     Start multiple nodes on a single server. All nodes will try to join on the first node started.\n");
            Console.WriteLine("  `chord benchmark <hostname:port> [hostname:port ...]");
            Console.WriteLine("     Run benchmarks against a list of nodes.\n");
            return;
        }

        logger.Debug($"Launched with arguments: {string.Join(" ", args)}");

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

            case "testserve":
                Task.Run(() => Testserve(args));
                break;
        }

        logger.Info("Running for 15 minutes at most");
        Task.Delay(36000000).Wait();
        logger.Info("30 minutes have elapsed, terminating application");
    }

    static void Serve(string[] args)
    {
        var logger = Dev.Logger($"Serve({string.Join(" ", args)})");
        if (args.Length < 4)
        {
            SyntaxError("At least 3 arguments required");
            return;
        }
        var hostname = args[1];
        var port = int.Parse(args[2]);
        var nodeName = args[3] ?? null;
        string? nodeToJoin = null;
        if (args.Length > 4)
        {
            nodeToJoin = args[4];
        }

        if (nodeName == null)
        {
            SyntaxError("Required argument 3 (node name) not provided");
            return;
        }

        logger.Debug($"Launched with arguments hostname={hostname} port={port} nodeName={nodeName} nodeToJoin={nodeToJoin}");

        Task.Run(async () => await RunWebServer(hostname, port, nodeName, nodeToJoin));
        logger.Debug("RunWebServer done");

        void SyntaxError(string message)
        {
            logger.Error(message);
            logger.Info($"Syntax: {args[0]} <hostname> <port number> <node name> [node to join]");
        }
    }

    static void Testserve(string[] args)
    {
        var logger = Dev.Logger($"Serve({string.Join(" ", args)})");
        if (args.Length < 4)
        {
            SyntaxError("At least 3 arguments required");
            return;
        }
        var hostname = args[1];
        var port = int.Parse(args[2]);
        var nodeCountString = args[3] ?? null;
        if (nodeCountString == null)
        {
            logger.Error("Requires a node count");
            Environment.Exit(2);
            return;
        }
        var nodeCount = int.Parse(nodeCountString);

        var taskList = new List<Task>();
        for (int nodeNumber = 0; nodeNumber <= nodeCount; nodeNumber++)
        {
            taskList.Add(RunWebServer(hostname, port + nodeNumber, $"{hostname}:{port + nodeNumber}"));
        }

        Task.WaitAny(taskList.ToArray());

        logger.Debug($"At least one node terminated");
        Environment.Exit(0);
        return;

        void SyntaxError(string message)
        {
            logger.Error(message);
            logger.Info($"Syntax: {args[0]} <hostname> <port number> <node name> [node to join]");
        }
    }

    /**
     *  ChordDHT serve c3-20 8080 c3-21:8080 c3-22:8080 ...
     *  blir til
     *  ChordDHT serve c3-20 8080 <existing_network_to_join>
     */

    static void Multiserve(string[] args)
    {
        var logger = Dev.Logger($"Multiserve({string.Join(" ", args)})");
        var hostname = args[1];
        var port = int.Parse(args[2]);
        var nodeCount = int.Parse(args[3]);
        if (nodeCount < 1)
        {
            logger.Fatal("Must run at least one node");
            return;
        }

        var nodeList = new string[nodeCount];
        nodeList[0] = $"{hostname}:{port}";
        for (int i = 1; i < nodeCount; i++)
        {
            nodeList[i] = $"{hostname}:{port + i}";
        }

        logger.Info($"Node list: {string.Join(" ", nodeList)}");


        Task[] tasks = new Task[nodeCount];

        logger.Info("Starting first instance...");
        tasks[0] = Task.Run(async () =>
        {
            try
            {
                await RunWebServer(hostname, port, $"{nodeList[0]}");
            }
            catch (Exception ex)
            {
                logger.Fatal($"Error in primary node ({hostname}:{port}:\n{ex}");
            }
            logger.Debug($"Instance {hostname}:{port} terminated");
        });

        var primaryNode = $"{hostname}:{port}";
        for (int i = 1; i < nodeCount; i++)
        {
            var nodeName = nodeList[i];
            var portNumber = port + i;
            tasks[i] = Task.Run(async () =>
            {
                try
                {
                    await RunWebServer(hostname, portNumber, nodeName, primaryNode);
                }
                catch (Exception ex)
                {
                    logger.Fatal($"Error in node ({hostname}:{port}:\n{ex}");
                }
                logger.Debug($"Instance {hostname}:{port} terminated");
            });

        }
    }

    static async void Benchmark(string[] args)
    {
        List<Dictionary<string,string>> reports = new List<Dictionary<string, string>>();
        
        var nodeCount = args.Length - 1;
        if (nodeCount < 1)
        {
            Console.WriteLine("Must benchmark at least one node");
            return;
        }
        int repetitions = 50;
        int workerThreads = 1024;
        var nodeList = new string[nodeCount];
        for (int i = 0; i < nodeCount; i++)
        {
            nodeList[i] = args[i + 1];
        }
        await Task.Delay(2000);

        HttpClientHandler handler = new HttpClientHandler();
        handler.MaxConnectionsPerServer = 64;
        HttpClient httpClient = new HttpClient(handler);
        httpClient.Timeout = TimeSpan.FromSeconds(30);
        httpClient.MaxResponseContentBufferSize = 65536;
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
            countEvent($"HTTP {(int)response.StatusCode} {response.ReasonPhrase}");
            if (hopCount.HasValue)
            {
                return $"{hopCount} hops";
            }
            else
            {
                return null;
            }
        };
        var dumpExtraStatistics = () =>
        {
            Dictionary<string, string> results = new Dictionary<string, string>();
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
                results.Add($"{i}_hops", hopCount.ToString());
                Console.WriteLine($"  {i,2} hops: {hopCount,4}");
            }
            if (eventCounters.Count > 0)
            {
                Console.WriteLine();
                Console.WriteLine(" OTHER COUNTERS:");
                foreach (var key in eventCounters.Keys)
                {
                    Console.WriteLine($"  {key + ":",20} {eventCounters[key]}");
                }
            }
            Console.WriteLine();
            hopCounters = new ConcurrentDictionary<int, int>();
            eventCounters = new ConcurrentDictionary<string, int>();
            return results;
        };

        reports.Add(await new Benchmark("GET requests for random keys on random hosts", async () => {
            var response = await httpClient.GetAsync(randomNodeAndKey());
            return processHttpClientResponse(response);
        }, dumpExtraStatistics).Run(repetitions, workerThreads));

        reports.Add(await new Benchmark("PUT requests for random keys on random hosts", async () => {
            var requestBody = new StringContent("Lorem Ipsum Dolor Sit Amet");
            requestBody.Headers.ContentType = MediaTypeHeaderValue.Parse("text/plain");
            var response = await httpClient.PutAsync(randomNodeAndKey(), requestBody);
            return processHttpClientResponse(response);
        }, dumpExtraStatistics).Run(repetitions, workerThreads));

        // Generate CSV data
        Console.WriteLine("CSV DATA:");
        List<string> captions = new List<string>();
        foreach (var report in reports)
        {
            foreach (var caption in report.Keys)
            {
                captions.Add(caption);
            }
            break;
        }
        Console.WriteLine(string.Join(";", captions));
        foreach (var report in reports)
        {
            Console.WriteLine(string.Join(";", report.Values));
        }
    }

    static async Task RunWebServer(string hostname, int port, string nodeName, string? nodeToJoin=null)
    {
        var logger = Dev.Logger($"RunWebServer({hostname}:{port})");
        try
        {
            if (nodeName==null) throw new NullReferenceException(nameof(nodeName));
            logger.Info($"Starting DHTServer (hostname={hostname} port={port} nodeName={nodeName} nodeToJoin={nodeToJoin})");
            var dhtServer = new DHTServer(nodeName, new DictionaryStorageBackend());

            dhtServer.Router.AddRoute(
                new Route("GET", "/frode", async (context) =>
                {
                    await context.Send.Ok("Known nodes: " + string.Join(", ", dhtServer.Chord.GetKnownNodes().Select(n => n.Name)));
                })
            );


            // Whenever true stabilization will run regularly
            bool runStabilization = true;

            /**
             * Simulating crash of the server
             */
            SetupCrashHandling(dhtServer, logger, () => {
                // Crash is being simulated
                runStabilization = false;
                dhtServer.ResetState();
            }, () => { 
                // Crash no longer being simulated
                runStabilization = true;
            });

            var runTask = dhtServer.Run();

            /**
             * Handle joining an existing network
             */
            if (nodeToJoin != null)
            {
                logger.Info($"Requesting to join chord network via {nodeToJoin}");
                await dhtServer.JoinNetwork(nodeToJoin);
            }

            while (!runTask.IsCompleted)
            {
                if (runStabilization)
                {
                    await dhtServer.RunStabilization();
                }
                await Task.Delay(Util.RandomInt(300, 600));
            }
            await runTask;
        }
        catch (Exception ex)
        {
            logger.Error($"Got exception:\n{ex}");
        }
    }

    static void SetupCrashHandling(DHTServer dhtServer, ILogger logger, Action onCrashSimEnabled, Action onCrashSimDisabled)
    {
        List<Route> routes = new List<Route>
        {
            new Route("PUT", "/chord-node-api", new RequestDelegate(SimulatedCrashHandler), 100),
            new Route("GET", $"/node-info", new RequestDelegate(SimulatedCrashHandler), 100),
            new Route("GET", $"/storage/(?<key>\\w+)", new RequestDelegate(SimulatedCrashHandler), 100),
            new Route("PUT", $"/storage/(?<key>\\w+)", new RequestDelegate(SimulatedCrashHandler), 100),
            new Route("DELETE", $"/storage/(?<key>\\w+)", new RequestDelegate(SimulatedCrashHandler), 100),
            new Route("OPTIONS", $"/storage/(?<key>\\w+)", new RequestDelegate(SimulatedCrashHandler), 100),
            new Route("GET|POST", "/leave", new RequestDelegate(SimulatedCrashHandler), 100),
            new Route("GET|POST", "/join", new RequestDelegate(SimulatedCrashHandler), 100)
        };

        List<string>? nodesForRejoin = null;

        Route? resetRoute = null;

        RequestDelegate simRestoreHandler = async context =>
        {
            if (nodesForRejoin == null)
            {
                await context.Send.JSON("I wasn't simulating a crash. Perhaps I actually crashed?");
            }
            else
            {
                onCrashSimDisabled();
                dhtServer.Logger.Debug("Crashed Chord Node simulation disabled");
                foreach (var route in routes)
                {
                    dhtServer.Router.RemoveRoute(route);
                }
                if (nodesForRejoin.Count == 0)
                {
                    await context.Send.Ok("Node recovered");
                    return;
                }
                foreach (var rejoinCandidate in nodesForRejoin)
                {
                    logger.Info($"Trying to rejoin via {rejoinCandidate}");
                    if (rejoinCandidate == dhtServer.Chord.Node.Name)
                    {
                        continue;
                    }
                    try
                    {
                        await dhtServer.JoinNetwork(rejoinCandidate);
                        logger.Info($"Node rejoined network via {rejoinCandidate}");
                        nodesForRejoin = null;
                        await context.Send.JSON("Crash simulation stopped");
                        return;
                    }
                    catch (Exception)
                    {
                        logger.Info($"Node could not rejoin network via {rejoinCandidate}");
                    }
                }
                await context.Send.Ok($"Node recovered but could not rejoin the network, tried to connect via {string.Join(", ", nodesForRejoin)}.");
            }
        };

        RequestDelegate simCrashHandler = async context =>
        {
            if (nodesForRejoin != null)
            {
                await context.Send.JSON("Already simulating a crash");
            }
            else
            {
                resetRoute = new Route("GET", "/reset", async (context) => {
                    await dhtServer.Chord.ResetState();
                    await simRestoreHandler(context);
                    if (resetRoute != null)
                    {
                        dhtServer.Router.RemoveRoute(resetRoute);
                    }
                    resetRoute = null;
                }, 100);
                dhtServer.Router.AddRoute(resetRoute);
                nodesForRejoin = new List<string>(dhtServer.Chord.GetKnownNodes().Select(n => n.Name));
                onCrashSimEnabled();
                dhtServer.Logger.Debug("Crashed Chord Node simulation enabled");
                foreach (var route in routes)
                {
                    dhtServer.Router.AddRoute(route);
                }
                dhtServer.ResetState();
                await context.Send.JSON($"Simulating a crash: Remembering {string.Join(", ", nodesForRejoin)}");
            }
        };

        dhtServer.Router.AddRoute(new Route("GET|POST", $"/sim-crash", simCrashHandler));
        dhtServer.Router.AddRoute(new Route("GET|POST", $"/sim-recover", simRestoreHandler));

    }

    static async Task SimulatedCrashHandler(HttpContext context)
    {
        await Task.Delay(1000);
        await context.Send.InternalServerError();
    }

}
