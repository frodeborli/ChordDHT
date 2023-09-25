using ChordDHT.ChordProtocol;
using ChordDHT.Util;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace ChordDHT.DHT
{
    public class DHTServer : DHTClient
    {
        private readonly IStorageBackend StorageBackend;
        private readonly Router Router;
        protected Chord ChordProtocol;
        public int LastRequestHops { get; private set; } = 0;

        public DHTServer(string nodeName, string[]? nodeList, IStorageBackend storageBackend, Router router, string prefix = "/")
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
            Router = router;
            Router.AddRoute(new Route("GET", $"{Prefix}info", new RequestHandler(GetInfoHandler)));
            Router.AddRoute(new Route("GET", $"{Prefix}neighbors", new RequestHandler(GetNeighborsHandler)));
            Router.AddRoute(new Route("GET", $"{Prefix}(?<key>\\w+)", new RequestHandler(GetHandler)));
            Router.AddRoute(new Route("PUT", $"{Prefix}(?<key>\\w+)", new RequestHandler(PutHandler)));
            Router.AddRoute(new Route("DELETE", $"{Prefix}(?<key>\\w+)", new RequestHandler(DeleteHandler)));
            Router.AddRoute(new Route("OPTIONS", $"{Prefix}(?<key>\\w+)", new RequestHandler(OptionsHandler)));
        }

        public void JoinNetwork(string nodeName)
        {
            throw new NotImplementedException();
        }

        /**
         * Handles requests to {Prefix}/info and returns some information about the
         * node.
         */
        private async Task GetInfoHandler(HttpListenerContext context, RequestVariables? variables)
        {
            var info = new
            {
                nodeId = ChordProtocol.NodeId,
                nodeName = ChordProtocol.NodeName,
                knownNodes = ChordProtocol.KnownNodes,
                predecessorNode = ChordProtocol.PredecessorNode,
                successorNode = ChordProtocol.SuccessorNode,
                fingers = ChordProtocol.Fingers
            };
            await Send.JSON(context, info);
        }

        /**
         * Handles GET requests to {Prefix}/neighbors and returns the predecessor node and
         * successor node
         */
        private async Task GetNeighborsHandler(HttpListenerContext context, RequestVariables? variables)
        {
            await Send.JSON(context, new
            {
                predecessor = ChordProtocol.PredecessorNode,
                successor = ChordProtocol.SuccessorNode
            });
        }

        /**
         * Handles GET requests to {Prefix}{Key} and returns the value at the
         * location with the correct content type, or 404.
         */
        private async Task GetHandler(HttpListenerContext context, RequestVariables? variables)
        {
            var key = variables["key"];
            IStoredItem? result = await Get(key);

            context.Response.AppendHeader("X-Chord-Hops", LastRequestHops.ToString());
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
                Router.SendPageNotFound(context);
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

            if (await Put(key, item))
            {
                context.Response.AppendHeader("X-Chord-Hops", LastRequestHops.ToString());
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
            if (await Remove(key))
            {
                context.Response.AppendHeader("X-Chord-Hops", LastRequestHops.ToString());
                new GenericStatusRequestHandler(200, "Ok").HandleRequest(context);
            } else
            {
                new GenericStatusRequestHandler(HttpStatusCode.Conflict, "Could not delete");
            }
        }

        new public async Task<IStoredItem?> Get(string key)
        {
            var bestNode = ChordProtocol.Lookup(key);
            if (bestNode == NodeName)
            {
                return await StorageBackend.Get(key);
            } else
            {
                var url = await FindNode(key);
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
                    return item;
                } else
                {
                    return null;
                }
            }
        }

        new public async Task<bool> Put(string key, IStoredItem value)
        {
            var bestNode = ChordProtocol.Lookup(key);
            if (bestNode == NodeName)
            {
                return await StorageBackend.Put(key, value);
            }
            else
            {
                var nextUrl = await FindNode(key);

                var requestBody = new ByteArrayContent(value.Data);
                requestBody.Headers.ContentType = new System.Net.Http.Headers.MediaTypeHeaderValue(value.ContentType);

                var response = await HttpClient.PutAsync(nextUrl, requestBody);
                if (response.IsSuccessStatusCode)
                {
                    return true;
                }
                else
                {
                    throw new InvalidOperationException($"Invalid response from node at {nextUrl} (statusCode={response.StatusCode})");
                }
            }
        }

        new public async Task<bool> Remove(string key)
        {
            var bestNode = ChordProtocol.Lookup(key);
            if (bestNode == NodeName)
            {
                return await StorageBackend.Remove(key);
            } else
            {
                var url = await FindNode(key);
                var response = await HttpClient.DeleteAsync(url);
                if (response.IsSuccessStatusCode)
                {
                    return true;
                }
                else
                {
                    return false;
                }
            }
        }

        /**
         * Finds the node which is responsible for storing the key and
         * returns the final direct URL for the key.
         */
        public async Task<string> FindNode(string key)
        {
            LastRequestHops = 0;
            var bestNode = ChordProtocol.Lookup(key);

            var nextUrl = $"http://{bestNode}{Prefix}{key}";

            // Find the node responsible for this key
            for (; ; )
            {
                LastRequestHops++;
                var requestMessage = new HttpRequestMessage(HttpMethod.Options, nextUrl);
                var response = await HttpClient.SendAsync(requestMessage);

                if (response.IsSuccessStatusCode)
                {
                    return nextUrl;
                }
                else if (response.StatusCode == HttpStatusCode.RedirectKeepVerb && response.Headers?.Location != null)
                {
                    // The request should be repeated at another node
                    nextUrl = response.Headers.Location?.ToString();
                }
                else
                {
                    throw new InvalidOperationException($"Invalid response from node at {nextUrl} (statusCode={response.StatusCode})");
                }
            }
        }
    }
}
