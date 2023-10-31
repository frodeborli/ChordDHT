using ChordDHT.ChordProtocol.Exceptions;
using ChordDHT.Fubber;
using ChordProtocol;
using Fubber;
using System.Net;
using System.Runtime.InteropServices;
using System.Text.Json;
using System.Web;

namespace ChordDHT.DHT
{
    public class DHTServer : WebApp
    {
        private readonly string NodeName;
        private readonly IStorageBackend StorageBackend;

        // Made this public because we need to simulate crashing crap
        public Chord Chord;

        // If a node is in the process of joining
        protected string? JoiningNode = null;

        private DHTNetworkAdapter NetworkAdapter;

        public DHTServer(string nodeName, IStorageBackend storageBackend, ILogger? logger = default, string prefix = "/")
            : base($"http://{nodeName}", logger ?? Dev.Logger(nodeName))
        {
            NodeName = nodeName;
            StorageBackend = storageBackend;
            NetworkAdapter = new DHTNetworkAdapter(this, Logger.Logger("NetworkAdapter"), Chord);
            Chord = new Chord(nodeName, NetworkAdapter, Logger.Logger("Chord"));
            Router.AddRoute(new Route[] {
                new Route("GET", $"/node-info", RequestNodeInfo),
                new Route("GET", $"/storage/(?<key>[^/]+)", RequestStorageKeyGet),
                new Route("PUT", $"/storage/(?<key>[^/]+)", RequestStorageKeyPut),
                new Route("DELETE", $"/storage/(?<key>[^/]+)", RequestStorageKeyDelete),
                new Route("OPTIONS", $"/storage/(?<key>[^/]+)", RequestStorageKeyOptions),
                new Route("POST|GET", $"/join", RequestJoin),
                new Route("POST|GET", $"/leave", RequestLeave),
                new Route("GET", $"/debug", RequestDebug),
                new Route("GET", "/all-nodes", GetAllNodesRequestHandler)
            });
        }

        public void Detach() => Chord.Detach();

        public async Task RunStabilization()
        {
            await Chord.RunStabilization();
        }

        public Task JoinNetwork(string nodeName) => Chord.JoinNetwork(new Node(nodeName, Chord.Hash(nodeName)));
        public Task LeaveNetwork() => Chord.LeaveNetwork();

        private string GetNodeUrl(string nodeName, string endpoint)
        {
            return $"http://{nodeName}{endpoint}";
        }

        private string NodeUrl(Node node, string endpoint) => GetNodeUrl(node.Name, endpoint);

        private async Task<NodeInfo> GetNodeInfo(string nodeName)
        {
            var response = await HttpClient.GetAsync(GetNodeUrl(nodeName, "node-info"));
            response.EnsureSuccessStatusCode();
            var jsonString = await response.Content.ReadAsStringAsync();
            var nodeInfo = JsonSerializer.Deserialize<NodeInfo>(jsonString);
            if (nodeInfo == null)
            {
                throw new InvalidDataException($"Received invalid data from 'http://{nodeName}/node-info'");
            }
            return nodeInfo;
        }

        /**
         * Return node information
         */
        private async Task RequestNodeInfo(HttpContext context)
        {

            await context.Send.JSON(new NodeInfo
            {
                NodeHash = Chord.Node.Hash.ToString("X"),
                NodeName = Chord.Node.Name,
                KnownNodes = Chord.GetKnownNodes().Select(n => n.Name).ToArray(),
                Predecessor = Chord.PredecessorNode.Name,
                Successor = Chord.SuccessorNode.Name,
            });
        }

        /**
         * Return node information
         */
        private async Task RequestDebug(HttpContext context)
        {
            string debugInfo = Chord.GetDebugInfo();

            var messages = Logger.GetMessages();

            if (messages != null)
            {
                debugInfo = $"{debugInfo}\n" +
                    $"\n" +
                    $"Log:\n" +
                    $"====\n" +
                    $"\n" +
                    $"{string.Join("\n", messages)}\n\n";
            }


            if (StorageBackend is DictionaryStorageBackend dictStorageBackend)
            {
                var keys = dictStorageBackend.GetDictionary().Keys.ToArray();
                var storedKeys = string.Join(", ", keys);
                debugInfo = $"{debugInfo}\n" +
                    $"" +
                    $"Stored Keys:\n" +
                    $"------------\n" +
                    $"{storedKeys}\n" +
                    $"\n";
            }

            await context.Send.Ok(debugInfo);
        }

        public async Task GetAllNodesRequestHandler(HttpContext context)
        {
            throw new NotImplementedException();
        }

        /**
         * Handles GET requests to {Prefix}{Key} and returns the value at the
         * location with the correct content type, or 404.
         */
        private async Task RequestStorageKeyGet(HttpContext context)
        {
            var key = context.RouteVariables["key"];
            Logger.Debug($"Resolving key '{key}' from client");
            var (result, hopCount) = await GetReal(key);
            context.Response.AppendHeader("X-Chord-Hops", hopCount.ToString());

            if (result != null)
            {
                context.Response.ContentType = result.ContentType;
                context.Response.StatusCode = 200;
                await context.Response.OutputStream.WriteAsync(result.Data, 0, result.Data.Length);
                await context.Response.OutputStream.FlushAsync();
                return;
            }
            else
            {
                await context.Send.NotFound();
            }
        }

        /**
         * Handles OPTIONS requests to {Prefix}{Key} and returns 200 Ok if the node
         * is responsible for handling the key, or a 307 Redirect if another node
         * is responsible for the key.
         */
        private async Task RequestStorageKeyOptions(HttpContext context)
        {
            var key = context.RouteVariables["key"];
            var node = await Chord.QuerySuccessor(key);
            if (node.Name == NodeName)
            {
                // This node is responsible for the key
                await context.Send.JSON(true);
            } else
            {
                await context.Send.TemporaryRedirect(NodeUrl(node, $"/storage/{key}"));
            }
        }

        /**
         * When this handler is invoked, the node should join a network and receive all the relevant keys
         * that it will be responsible for storing.
         */
        private async Task RequestJoin(HttpContext context)
        {
            if (Chord.Node != Chord.PredecessorNode)
            {
                await context.Send.Conflict("Already part of a chord network");
                return;
            }

            var nprime = context.Request.QueryString["nprime"];
            if (nprime == null)
            {
                await context.Send.BadRequest("The query parameter nprime is required");
                return;
            }


            try
            {
                await Chord.JoinNetwork(nprime);
            } 
            catch (Exception ex)
            {
                Logger.Notice($"Tried to join node {nprime} but got error: {ex}");
                await context.Send.InternalServerError($"Unable to join network at {nprime}");
                return;
            }
            await context.Send.JSON($"Network of '{nprime}' joined");
        }

        /**
         * When this handler is invoked, the node will gracefully leave the network and become
         * a "loner" node. All keys should be copied to the successor node, and then the predecessor
         * node must be informed that this node no longer exists.
         */
        private async Task RequestLeave(HttpContext context)
        {
            if (Chord.Node == Chord.PredecessorNode)
            {
                // await context.Send.Ok($"I would rather send HTTP 409 Conflict, since I am not participating in a network, but the test script from UiT says that we must return HTTP 200 Ok");
                await context.Send.Ok("Not currently part of any network (WOULD RATHER SEND HTTP 409 BUT I WAS NOT ALLOWED TO DO SO BY MY TEACHER)");
                return;
            }
            await Chord.LeaveNetwork();
            await context.Send.Ok($"Left the chord network");
        }

        /**
         * Handles PUT requests to {Prefix}{Key} and stores the value in the distributed
         * hash table.
         */
        private async Task RequestStorageKeyPut(HttpContext context)
        {
            var key = context.RouteVariables["key"];
            Stream inputStream = context.Request.InputStream;
            MemoryStream memoryStream = new MemoryStream();
            byte[] buffer = new byte[4096];
            int bytesRead;

            while ((bytesRead = await inputStream.ReadAsync(buffer, 0, buffer.Length)) > 0)
            {
                await memoryStream.WriteAsync(buffer, 0, bytesRead);
            }
            var body = memoryStream.ToArray();
            var item = new StoredItem(context.Request.ContentType ?? "text/plain", body);

            var (result, hopCount) = await PutReal(key, item);
            context.Response.AppendHeader("X-Chord-Hops", hopCount.ToString());
            if (result)
            {
                await context.Send.Ok();
            }
            else
            {
                await context.Send.Conflict("Could not store");
            }
        }

        /**
         * Handles DELETE requests to {Prefix}{Key} and performes a delete operation in
         * the DHT.
         */
        private async Task RequestStorageKeyDelete(HttpContext context)
        {
            var key = context.RouteVariables["key"];
            var (result, hopCount) = await RemoveReal(key);
            context.Response.AppendHeader("X-Chord-Hops", hopCount.ToString());
            if (result)
            {
                await context.Send.Ok();
            } else
            {
                await context.Send.Conflict("Could not delete");
            }
        }

        public async Task<IStoredItem?> Get(string key)
        {
            if (key == null) throw new NullReferenceException(nameof(key));
            var (result, hopCount) = await GetReal(key);
            return result;
        }

        private async Task<(IStoredItem?, int)> GetReal(string key)
        {
            if (key == null) throw new NullReferenceException(nameof(key));
            var bestNode = await Chord.QuerySuccessor(key);
            if (bestNode == Chord.Node)
            {
                Logger.Debug($"Getting from storage backend the key {key}");
                return (await StorageBackend.Get(key), 0);
            } else
            {
                var (url, hopCount) = await QueryKeyWithHopCount(key);
                HttpResponseMessage response = await HttpClient.GetAsync(url);
                if (response.IsSuccessStatusCode)
                {
                    // We got the data we wanted
                    var jsonString = await response.Content.ReadAsStringAsync();
                    StoredItem item;
                    if (response.Content.Headers.ContentType != null)
                    {
                        item = new StoredItem(response.Content.Headers.ContentType.ToString(), await response.Content.ReadAsByteArrayAsync());
                    } else
                    {
                        item = new StoredItem(await response.Content.ReadAsStringAsync());
                    }
                    item.NodeHopCounter = hopCount;
                    return (item, hopCount);
                } else
                {
                    return (null, hopCount);
                }
            }
        }

        public async Task<bool> Put(string key, IStoredItem value)
        {
            var (result, hopCount) = await PutReal(key, value);
            return result;
        }

        private async Task<(bool, int)> PutReal(string key, IStoredItem value)
        {
            var bestNode = await Chord.QuerySuccessor(key);
            if (bestNode == Chord.Node)
            {
                return (await StorageBackend.Put(key, value), 0);
            }
            else
            {
                var (url, hopCount) = await QueryKeyWithHopCount(key);
                value.NodeHopCounter = hopCount;

                var requestBody = new ByteArrayContent(value.Data);
                requestBody.Headers.ContentType = new System.Net.Http.Headers.MediaTypeHeaderValue(value.ContentType);

                var response = await HttpClient.PutAsync(url, requestBody);
                if (response.IsSuccessStatusCode)
                {
                    return (true, hopCount);
                }
                else
                {
                    throw new InvalidOperationException($"Invalid response from 'PUT {url}' (statusCode={response.StatusCode})");
                }
            }
        }

        public async Task<bool> Remove(string key)
        {
            var (result, hopCount) = await RemoveReal(key);
            return result;
        }

        private async Task<(bool, int)> RemoveReal(string key)
        {
            var bestNode = await Chord.QuerySuccessor(key);
            if (bestNode == Chord.Node)
            {
                return (await StorageBackend.Remove(key), 0);
            } else
            {
                var (url, hopCount) = await QueryKeyWithHopCount(key);
                var response = await HttpClient.DeleteAsync(url);
                if (response.IsSuccessStatusCode)
                {
                    return (true, hopCount);
                }
                else
                {
                    return (false, hopCount);
                }
            }
        }

        public async Task<string> QueryKey(string key)
        {
            var (url, hopCount) = await QueryKeyWithHopCount(key);
            var uri = new Uri(url);
            return uri.Host + ":" + uri.Port;
        }

        /**
         * Finds the url which is responsible for storing the key and
         * returns a tuple with the final direct URL and the hop count.
         */
        public async Task<(string, int)> QueryKeyWithHopCount(string key)
        {
            var bestNode = await Chord.QuerySuccessor(key);

            if (bestNode == Chord.Node)
            {
                throw new InvalidOperationException("Don't use QueryKeyWithHopCount() when the current node is the correct node");
            }

            int hopCount = 0;
            var nextUrl = NodeUrl(bestNode, $"/storage/{key}");

            // Find the node responsible for this key
            for (; ; )
            {
                hopCount++;
                var requestMessage = new HttpRequestMessage(HttpMethod.Options, nextUrl);
                var response = await HttpClient.SendAsync(requestMessage);

                if (response.IsSuccessStatusCode)
                {
                    return (nextUrl, hopCount);
                }
                else if (response.StatusCode == HttpStatusCode.RedirectKeepVerb && response.Headers?.Location != null)
                {
                    // The request should be repeated at another node
                    if (response.Headers.Location == null)
                    {
                        throw new InvalidOperationException($"The node at {nextUrl} responded with a redirect but without a 'Location' header");
                    } else
                    {
                        nextUrl = response.Headers.Location.ToString();
                    }
                }
                else
                {
                    throw new InvalidOperationException($"Invalid response from 'OPTIONS {nextUrl}' (statusCode={response.StatusCode})");
                }
            }
        }
    }
}
