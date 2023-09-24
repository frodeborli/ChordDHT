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

        public DHTServer(string nodeName, string[]? nodeList, IStorageBackend storageBackend, Router router, string prefix = "/")
            : base(nodeName, prefix)
        {
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
            Router.AddRoute(new Route("OPTIONS", $"{Prefix}(?<key>\\w+)", new RequestHandler(OptionsHandler)));
        }

        public void JoinNetwork(string nodeName)
        {
            // TODO: Add nodename to ChordProtocol and notify the other node about our existence.
        }

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

        private async Task GetNeighborsHandler(HttpListenerContext context, RequestVariables? variables)
        {
            await Send.JSON(context, new
            {
                predecessor = ChordProtocol.PredecessorNode,
                successor = ChordProtocol.SuccessorNode
            });
        }

        private async Task GetHandler(HttpListenerContext context, RequestVariables? variables)
        {
            var key = variables["key"];
            IStoredItem? result;

            result = await Get(key);
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
         * OPTIONS /prefix/{key} for checking if the node is responsible for storing the key or not
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
            var item = new StoredItem(context.Request.ContentType ?? "application/octet-stream", body);

            if (await Put(key, item))
            {
                context.Response.AppendHeader("X-Chord-Hops", LastRequestHops.ToString());
                new GenericStatusRequestHandler(200, "Ok").HandleRequest(context);
            }
            else
            {
                new GenericStatusRequestHandler(HttpStatusCode.Conflict, "Conflict").HandleRequest(context);
            }
            return;
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
                return await base.Remove(key);
            }
        }

    }
}
