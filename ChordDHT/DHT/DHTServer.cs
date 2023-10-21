﻿using ChordDHT.ChordProtocol;
using ChordDHT.Fubber;
using ChordProtocol;
using Fubber;
using System;
using System.Net;
using System.Text.Json;

namespace ChordDHT.DHT
{
    public class DHTServer : WebApp
    {
        private readonly string NodeName;
        private readonly IStorageBackend StorageBackend;
        protected Chord ChordProtocol;
        private bool IsPartOfNetwork = false;

        // If a node is in the process of joining
        protected string? JoiningNode = null;

        private Dev.LoggerContext Logger;
        private DHTNetworkAdapter DHTNetworkAdapter;


        public DHTServer(string nodeName, IStorageBackend storageBackend, string prefix = "/")
            : base($"http://{nodeName}")
        {
            NodeName = nodeName;
            Logger = Dev.Logger($"DHTServer {nodeName}");
            StorageBackend = storageBackend;
            DHTNetworkAdapter = new DHTNetworkAdapter(this);
            ChordProtocol = new Chord(nodeName, DHTNetworkAdapter);

            Router.AddRoute(new Route[] {
                new Route("GET", $"/node-info", GetNodeInfoHandler),
                new Route("GET", $"/storage/(?<key>[^/]+)", GetHandler),
                new Route("PUT", $"/storage/(?<key>[^/]+)", PutHandler),
                new Route("DELETE", $"/storage/(?<key>[^/]+)", DeleteHandler),
                new Route("OPTIONS", $"/storage/(?<key>[^/]+)", OptionsHandler),
                new Route("POST", $"/join", JoinHandler),
                new Route("POST", $"/leave", LeaveHandler),
                new Route("POST", $"/request-join", RequestJoinHandler)
            });
            Logger.Debug("Constructed");
        }

        private string NodeUrl(Node node, string endpoint)
        {
            return $"http://{node.Name}{endpoint}";
        }

        public async Task<bool> JoinNetwork(string masterNode)
        {
            if (IsPartOfNetwork)
            {
                return false;
            }
            Logger.Debug("Joining network {masterNode}");

            /**
             * Joining an existing network:
             * 
             * 1. OK Contact an existing node.
             * 2. OK Find my successor node.
             * 3. Request that the successor node take this node in as it's new predecessor.
             * 4. TODO Get a copy of all keys that will be stored by this node.
             * 5. TODO Notify the network about my presence.
             */

            // Since we are not already part of the network, we can't actually
            // query the network. Therefore we pretend to be part of the network
            // by replacing the ChordProtocol instance while we get the information
            // we need to join the network in the best possible way possible.
            ChordProtocol = new Chord(masterNode, new DHTNetworkAdapter(this), ChordProtocol.HashFunction);

            Logger.Debug("Searching for successor");
            // The successor is the node that would be responsible for storing the
            // key for our node.
            var ourSuccessor = await FindNode(ChordProtocol.Node.Name);

            Logger.Debug($"Found successor {ourSuccessor}, getting node info...");

            // Query the successor node to get useful information for joining
            var successorInfo = await GetNodeInfo(ourSuccessor);

            Logger.Debug($"Got successor info\n{Dev.FormatLong(successorInfo)}");

            // Learn about any existing nodes that we can find
            ChordProtocol.AddNode(successorInfo.Predecessor);
            ChordProtocol.AddNode(successorInfo.Successor);
            ChordProtocol.AddNode(successorInfo.NodeName);
            foreach (var nodeName in successorInfo.KnownNodes)
            {
                ChordProtocol.AddNode(nodeName);
            }

            var newChord = new Chord(NodeName, DHTNetworkAdapter, ChordProtocol.HashFunction);
            foreach (var node in ChordProtocol.KnownNodes)
            {
                newChord.AddNode(node.Name);
            }

            Logger.Debug($"Requesting to become the predecessor node for {ourSuccessor}");

            var response = await HttpClient.PostAsync(NodeUrl(ourSuccessor, "request-join"), new FormUrlEncodedContent(new Dictionary<string, string> {
                { "NodeName", NodeName }
            }));

            Console.WriteLine($"Changes needing to be done for {newChord.NodeName}:\n" +
                $" - Set our predecessor = {newChord.PredecessorNode}\n" +
                $" - Set our successor = {newChord.SuccessorNode}\n");

            // TODO FIRST: Copy all keys from them to us, when the keys belong to us.
            // TODO AFTER: Ask if we can replace their predecessor with ourselves
            return true;
        }

        public async Task<bool> LeaveNetwork()
        {
            if (!IsPartOfNetwork)
            {
                return false;
            }
            return true;
        }

        private async Task RequestJoinHandler(HttpContext context)
        {
            if (JoiningNode != null)
            {
                await context.Send.Conflict("Another node is currently in the process of joining");
                return;
            }



            await context.Send.NotImplemented();
        }

        private async Task<NodeInfo> GetNodeInfo(string nodeName)
        {
            var response = await HttpClient.GetAsync(NodeUrl(nodeName, "node-info"));
            response.EnsureSuccessStatusCode();
            var jsonString = await response.Content.ReadAsStringAsync();
            var nodeInfo = JsonSerializer.Deserialize<NodeInfo>(jsonString);
            if (nodeInfo == null)
            {
                throw new InvalidDataException($"Received invalid data from 'http://{nodeName}{Prefix}node-info'");
            }
            return nodeInfo;
        }

        /**
         * Return node information
         */
        private async Task GetNodeInfoHandler(HttpContext context)
        {
            await context.Send.JSON(new NodeInfo
            {
                NodeHash = ChordProtocol.Node.Hash.ToString("X"),
                NodeName = ChordProtocol.Node.Name,
                KnownNodes = ChordProtocol.KnownNodes.Select(n => n.Name).ToArray(),
                Predecessor = ChordProtocol.PredecessorNode.Name,
                Successor = ChordProtocol.SuccessorNode.Name,
            });
        }

        /**
         * Handles GET requests to {Prefix}{Key} and returns the value at the
         * location with the correct content type, or 404.
         */
        private async Task GetHandler(HttpContext context)
        {
            var key = context.Request.QueryString.Get("key") ?? string.Empty;
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
        private async Task OptionsHandler(HttpContext context)
        {
            var key = context.RouteVariables["key"];
            var nodeName = ChordProtocol.Lookup(key);
            if (nodeName == NodeName)
            {
                // This node is responsible for the key
                await context.Send.JSON(true);
            } else
            {
                await context.Send.TemporaryRedirect($"http://{nodeName}{Prefix}{key}");
            }
        }

        /**
         * When this handler is invoked, the node should join a network and receive all the relevant keys
         * that it will be responsible for storing.
         */
        private async Task JoinHandler(HttpContext context)
        {
            if (IsPartOfNetwork)
            {
                await context.Send.Conflict("Already part of a chord network");
                return;
            }
            if (!context.RouteVariables.ContainsKey("nprime"))
            {
                await context.Send.BadRequest("The query parameter nprime is required");
                return;
            }
            string nprime = context.RouteVariables["nprime"];
            if (await JoinNetwork(nprime))
            {
                await context.Send.JSON($"Network of '{nprime}' joined");
            } else
            {
                await context.Send.Conflict("Unable to join chord network");
            }
        }

        /**
         * When this handler is invoked, the node will gracefully leave the network and become
         * a "loner" node. All keys should be copied to the successor node, and then the predecessor
         * node must be informed that this node no longer exists.
         */
        private async Task LeaveHandler(HttpContext context)
        {
            if (!IsPartOfNetwork)
            {
                await context.Send.Conflict("Not currently part of any network");
                return;
            }
            if (await LeaveNetwork())
            {
                await context.Send.Ok($"Left the chord network");
            }
        }

        /**
         * Handles PUT requests to {Prefix}{Key} and stores the value in the distributed
         * hash table.
         */
        private async Task PutHandler(HttpContext context)
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
        private async Task DeleteHandler(HttpContext context)
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

        new public async Task<IStoredItem?> Get(string key)
        {
            var (result, hopCount) = await GetReal(key);
            return result;
        }

        new private async Task<(IStoredItem?, int)> GetReal(string key)
        {
            var bestNode = ChordProtocol.Lookup(key);
            if (bestNode == NodeName)
            {
                return (await StorageBackend.Get(key), 0);
            } else
            {
                var (url, hopCount) = await FindTarget(key);
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

        new public async Task<bool> Put(string key, IStoredItem value)
        {
            var (result, hopCount) = await PutReal(key, value);
            return result;
        }

        new private async Task<(bool, int)> PutReal(string key, IStoredItem value)
        {
            var bestNode = ChordProtocol.Lookup(key);
            if (bestNode == NodeName)
            {
                return (await StorageBackend.Put(key, value), 0);
            }
            else
            {
                var (url, hopCount) = await FindTarget(key);
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

        new public async Task<bool> Remove(string key)
        {
            var (result, hopCount) = await RemoveReal(key);
            return result;
        }

        new private async Task<(bool, int)> RemoveReal(string key)
        {
            var bestNode = ChordProtocol.Lookup(key);
            if (bestNode == NodeName)
            {
                return (await StorageBackend.Remove(key), 0);
            } else
            {
                var (url, hopCount) = await FindTarget(key);
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

        public async Task<string> FindNode(string key)
        {
            var (url, hopCount) = await FindTarget(key);
            var uri = new Uri(url);
            return uri.Host + ":" + uri.Port;
        }

        /**
         * Finds the url which is responsible for storing the key and
         * returns a tuple with the final direct URL and the hop count.
         */
        public async Task<(string, int)> FindTarget(string key)
        {
            var bestNode = ChordProtocol.Lookup(key);

            if (bestNode == ChordProtocol.Node)
            {
                throw new InvalidOperationException("Don't use FindNode() when the current node is the correct node");
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

                        // When walking the chord ring, we'll make a note of any nodes that we visit
                        var uri = new Uri(nextUrl);
                        var discoveredNode = uri.Host + ":" + uri.Port;
                        Dev.Debug($"Detected a node named '{discoveredNode}' when performing a lookup");
                        ChordProtocol.AddNode(discoveredNode);
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
