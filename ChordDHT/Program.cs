﻿using System.Collections.Concurrent;
using System.Net.Http.Headers;
using ChordDHT.Benchmark;
using ChordDHT.DHT;
using Fubber;

class Program
{
    public static void Main(string[] args)
    {
        ThreadPool.SetMinThreads(16, 64);
        if (args.Length == 0)
        {
            Console.WriteLine("Usage:");
            Console.WriteLine("  `chord serve <hostname> <port> [existing_node:port]`");
            Console.WriteLine("     Start a chord node and optionally join an existing network.");
            Console.WriteLine("  `chord multiserve <hostname> <start_port> <nodes_to_start>`");
            Console.WriteLine("     Start multiple nodes on a single server.");
            Console.WriteLine("     nodes_to_start is the total number of nodes, minimum 1.");
            Console.WriteLine("  `chord benchmark <hostname:port> [hostname:port ...]");
            Console.WriteLine("     Run benchmarks against a list of nodes.");
            return;
        }

        Dev.Debug($"Launched with arguments: {string.Join(" ", args)}");

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

        Console.WriteLine("Running for 15 minutes at most");
        Task.Delay(900000).Wait();
    }

    static void Serve(string[] args)
    {
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

        Dev.Debug($"Launched with arguments hostname={hostname} port={port} nodeName={nodeName} nodeToJoin={nodeToJoin}");

        Task.Run(async () => await RunWebServer(hostname, port, nodeName, nodeToJoin));
        Dev.Debug("RunWebServer done");

        void SyntaxError(string message)
        {
            Dev.Error(message);
            Dev.Info($"Syntax: {args[0]} <hostname> <port number> <node name> [node to join]");
        }
    }

    /**
     *  ChordDHT serve c3-20 8080 c3-21:8080 c3-22:8080 ...
     *  blir til
     *  ChordDHT serve c3-20 8080 <existing_network_to_join>
     */

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

        Dev.Info($"Node list: {string.Join(" ", nodeList)}");


        Task[] tasks = new Task[nodeCount];

        Console.WriteLine("Starting first instance...");
        tasks[0] = Task.Run(async () =>
        {
            try
            {
                await RunWebServer(hostname, port, $"{nodeList[0]}");
            }
            catch (Exception ex)
            {
                Dev.Error($"Error in primary node ({hostname}:{port}:\n{ex}");
            }
            Dev.Debug($"Instance {hostname}:{port} terminated");
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
                    Dev.Error($"Error in node ({hostname}:{port}:\n{ex}");
                }
                Dev.Debug($"Instance {hostname}:{port} terminated");
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
        handler.MaxConnectionsPerServer = 256;
        HttpClient httpClient = new HttpClient(handler);
        httpClient.MaxResponseContentBufferSize = 4096;
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
        var logger = Dev.Logger("RunWebServer");
        try
        {
            if (nodeName==null) throw new NullReferenceException(nameof(nodeName));
            logger.Info($"Starting DHTServer (hostname={hostname} port={port} nodeName={nodeName} nodeToJoin={nodeToJoin})");
            var dhtServer = new DHTServer(nodeName, new DictionaryStorageBackend());

            /**
             * Simulating crash of the server
             */
            SetupCrashHandling(dhtServer);

            var runTask = dhtServer.Run();

            /**
             * Handle joining an existing network
             */
            if (nodeToJoin != null)
            {
                logger.Info($"Requesting to join chord network via {nodeToJoin}");
                await dhtServer.JoinNetwork(nodeToJoin);
            }

            await runTask;
        } catch (Exception ex)
        {
            logger.Error($"Got exception:\n{ex}");
        }
    }

    static void SetupCrashHandling(WebApp webApp)
    {
        Route NodeInfoRoute = new Route("GET", $"/node-info", new RequestDelegate(SimulatedCrashHandler), 100);
        Route GetKeyRoute = new Route("GET", $"/storage/(?<key>\\w+)", new RequestDelegate(SimulatedCrashHandler), 100);
        Route PutKeyRoute = new Route("PUT", $"/storage/(?<key>\\w+)", new RequestDelegate(SimulatedCrashHandler), 100);
        Route DeleteKeyRoute = new Route("DELETE", $"/storage/(?<key>\\w+)", new RequestDelegate(SimulatedCrashHandler), 100);
        Route OptionsRoute = new Route("OPTIONS", $"/storage/(?<key>\\w+)", new RequestDelegate(SimulatedCrashHandler), 100);

        bool isSimulatingCrash = false;

        webApp.Router.AddRoute(new Route("GET", $"/sim-crash", async context => {
            if (isSimulatingCrash)
            {
                await context.Send.JSON("Already simulating a crash");
            } else
            {
                Dev.Debug("Crashed Chord Node simulation enabled");
                webApp.Router.AddRoute(NodeInfoRoute);
                webApp.Router.AddRoute(GetKeyRoute);
                webApp.Router.AddRoute(PutKeyRoute);
                webApp.Router.AddRoute(DeleteKeyRoute);
                webApp.Router.AddRoute(OptionsRoute);
                isSimulatingCrash = true;
                await context.Send.JSON("Simulating a crash");
            }
        }));
        webApp.Router.AddRoute(new Route("GET", $"/sim-recover", async context => {
            if (!isSimulatingCrash)
            {
                await context.Send.JSON("I wasn't simulating a crash. Perhaps I actually crashed?");
            }
            else
            {
                Dev.Debug("Crashed Chord Node simulation disabled");
                webApp.Router.RemoveRoute(NodeInfoRoute);
                webApp.Router.RemoveRoute(GetKeyRoute);
                webApp.Router.RemoveRoute(PutKeyRoute);
                webApp.Router.RemoveRoute(DeleteKeyRoute);
                webApp.Router.RemoveRoute(OptionsRoute);
                isSimulatingCrash = false;
                await context.Send.JSON("Stopping crash simulation");
            }
        }));

    }

    static async Task SimulatedCrashHandler(HttpContext context)
    {
        await Task.Delay(2000);
        await context.Send.InternalServerError();
    }

}
