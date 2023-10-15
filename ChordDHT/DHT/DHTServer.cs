using ChordDHT.ChordProtocol;
using ChordDHT.Util;
using System.Net;

namespace ChordDHT.DHT
{
    public class DHTServer : DHTClient
    {
        private readonly IStorageBackend StorageBackend;
        protected Chord ChordProtocol;

        private Route NodeInfoRoute, GetKeyRoute, PutKeyRoute, DeleteKeyRoute, OptionsRoute;

        public DHTServer(string nodeName, string[]? nodeList, IStorageBackend storageBackend, string prefix = "/")
            : base(nodeName, prefix)
        {
            ChordProtocol = new Chord(nodeName);
            if (nodeList != null)
            {
                foreach (string otherNode in nodeList)
                {
                    ChordProtocol.AddNode(otherNode);
                }
            }
            StorageBackend = storageBackend;

            NodeInfoRoute = new Route("GET", $"{Prefix}node-info", new RequestHandler(GetNodeInfoHandler));
            GetKeyRoute = new Route("GET", $"{Prefix}storage/(?<key>\\w+)", new RequestHandler(GetHandler));
            PutKeyRoute = new Route("PUT", $"{Prefix}storage/(?<key>\\w+)", new RequestHandler(PutHandler));
            DeleteKeyRoute = new Route("DELETE", $"{Prefix}storage/(?<key>\\w+)", new RequestHandler(DeleteHandler));
            OptionsRoute = new Route("OPTIONS", $"{Prefix}storage/(?<key>\\w+)", new RequestHandler(OptionsHandler));
        }

        public void RegisterRoutes(Router router)
        {
            router.AddRoute(NodeInfoRoute);
            router.AddRoute(GetKeyRoute);
            router.AddRoute(PutKeyRoute);
            router.AddRoute(DeleteKeyRoute);
            router.AddRoute(OptionsRoute);
        }

        public void UnregisterRoutes(Router router)
        {
            router.RemoveRoute(NodeInfoRoute);
            router.RemoveRoute(GetKeyRoute);
            router.RemoveRoute(PutKeyRoute);
            router.RemoveRoute(DeleteKeyRoute);
            router.RemoveRoute(OptionsRoute);
        }

        public void JoinNetwork(string nodeName)
        {
            throw new NotImplementedException();
        }

        /**
         * Return node information
         */
        private async Task GetNodeInfoHandler(HttpListenerContext context, RequestVariables? variables)
        {
            var info = new
            {
                node_hash = ChordProtocol.NodeId.ToString("X"),
                node_name = ChordProtocol.NodeName,
                others = ChordProtocol.KnownNodes,
                predecessor = ChordProtocol.PredecessorNode,
                successor = ChordProtocol.SuccessorNode,
            };
            await Send.JSON(context, info);
        }

        /**
         * Handles GET requests to {Prefix}{Key} and returns the value at the
         * location with the correct content type, or 404.
         */
        private async Task GetHandler(HttpListenerContext context, RequestVariables? variables)
        {
            var key = variables["key"];
            var (result, hopCount) = await GetReal(key);
            context.Response.AppendHeader("X-Chord-Hops", hopCount.ToString());

            if (result != null)
            {
                context.Response.ContentType = result.ContentType;
                context.Response.StatusCode = 200;
                await context.Response.OutputStream.WriteAsync(result.Data, 0, result.Data.Length);
                await context.Response.OutputStream.FlushAsync();
                context.Response.OutputStream.Close();
                context.Response.Close();
                return;
            }
            else
            {
                WebApp.Instance.Router.SendPageNotFound(context);
            }
        }

        /**
         * Handles OPTIONS requests to {Prefix}{Key} and returns 200 Ok if the node
         * is responsible for handling the key, or a 307 Redirect if another node
         * is responsible for the key.
         */
        private async Task OptionsHandler(HttpListenerContext context, RequestVariables? variables)
        {
            var key = variables["key"];
            var nodeName = ChordProtocol.Lookup(key);
            if (nodeName == NodeName)
            {
                // This node is responsible for the key
                new GenericStatusRequestHandler(200, "Ok").HandleRequest(context);
            } else
            {
                context.Response.AddHeader("Location", $"http://{nodeName}{Prefix}{key}");
                new GenericStatusRequestHandler(307, $"Redirect to http://{nodeName}{Prefix}{key}").HandleRequest(context);
            }
        }

        /**
         * Handles PUT requests to {Prefix}{Key} and stores the value in the distributed
         * hash table.
         */
        private async Task PutHandler(HttpListenerContext context, RequestVariables? variables)
        {
            var key = variables["key"];
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
                new GenericStatusRequestHandler(200, "Ok").HandleRequest(context);
            }
            else
            {
                new GenericStatusRequestHandler(HttpStatusCode.Conflict, "Could not store").HandleRequest(context);
            }
        }

        /**
         * Handles DELETE requests to {Prefix}{Key} and performes a delete operation in
         * the DHT.
         */
        private async Task DeleteHandler(HttpListenerContext context, RequestVariables? variables)
        {
            var key = variables["key"];
            var (result, hopCount) = await RemoveReal(key);
            context.Response.AppendHeader("X-Chord-Hops", hopCount.ToString());
            if (result)
            {
                new GenericStatusRequestHandler(200, "Ok").HandleRequest(context);
            } else
            {
                new GenericStatusRequestHandler(HttpStatusCode.Conflict, "Could not delete");
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
                var (url, hopCount) = await FindNode(key);
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
                var (url, hopCount) = await FindNode(key);
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
                    throw new InvalidOperationException($"Invalid response from node at {url} (statusCode={response.StatusCode})");
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
                var (url, hopCount) = await FindNode(key);
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

        /**
         * Finds the node which is responsible for storing the key and
         * returns a tuple with the final direct URL and the hop count.
         */
        public async Task<(string, int)> FindNode(string key)
        {
            var bestNode = ChordProtocol.Lookup(key);

            if (bestNode == NodeName)
            {
                throw new InvalidOperationException("Don't use FindNode() when the current node is the correct node");
            }

            int hopCount = 0;
            var nextUrl = $"http://{bestNode}{Prefix}{key}";

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
                    throw new InvalidOperationException($"Invalid response from node at {nextUrl} (statusCode={response.StatusCode})");
                }
            }
        }
    }
}
