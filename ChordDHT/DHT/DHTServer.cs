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
        private readonly string Prefix;

        public DHTServer(string nodeName, string[]? nodeList, IStorageBackend storageBackend, Router router, string prefix = "/")
            : base(nodeName)
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
            Prefix = prefix;
            Router.AddRoute(new Route("GET", $"{prefix}info", new RequestHandler(GetInfoHandler)));
            Router.AddRoute(new Route("GET", $"{prefix}neighbors", new RequestHandler(GetNeighborsHandler)));
            Router.AddRoute(new Route("GET", $"{prefix}(?<key>\\w+)", new RequestHandler(GetHandler)));
            Router.AddRoute(new Route("PUT", $"{prefix}(?<key>\\w+)", new RequestHandler(PutHandler)));
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
            var bestNode = ChordProtocol.lookup(key);
            IStoredItem? result;
            if (bestNode == NodeName)
            {
                // This node is responsible for the key
                result = await StorageBackend.Get(key);
                if (result != null)
                {
                    // We have data
                    var response = context.Response;
                    response.ContentType = result.contentType;
                    response.OutputStream.Write(result.data);
                    response.OutputStream.Close();
                } else {
                    // We don't have data
                    Router.SendPageNotFound(context);
                }
                return;
            }
            else
            {
                // Our node is not responsible for the key, so we'll redirect
                // the client.
                var betterUrl = $"http://{bestNode}{Prefix}{key}";
                var response = context.Response;
                response.StatusCode = (int) HttpStatusCode.RedirectKeepVerb;
                response.Headers.Add($"Location: {betterUrl}");
                response.ContentType = "application/json";
                string json = JsonSerializer.Serialize(new { redirect = betterUrl });
                response.OutputStream.Write(Encoding.UTF8.GetBytes(json));
                response.OutputStream.Close();
                return;
            }
        }

        private async Task PutHandler(HttpListenerContext context, RequestVariables? variables)
        {
            var key = variables["key"];
            var bestNode = ChordProtocol.lookup(key);
            if (bestNode == NodeName)
            {
                // We should store the request body locally
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
                if (await StorageBackend.Put(key, item))
                {
                    new GenericStatusRequestHandler(200, "Ok").HandleRequest(context);
                } else
                {
                    new GenericStatusRequestHandler(HttpStatusCode.Conflict, "Conflict").HandleRequest(context);
                }
                return;
            }
            else
            {
                // Our node is not responsible for the key, so we'll redirect
                // the client.
                var betterUrl = $"http://{bestNode}{Prefix}{key}";
                var response = context.Response;
                response.StatusCode = (int)HttpStatusCode.RedirectKeepVerb;
                response.Headers.Add($"Location: {betterUrl}");
                response.ContentType = "application/json";
                string json = JsonSerializer.Serialize(new { redirect = betterUrl });
                response.OutputStream.Write(Encoding.UTF8.GetBytes(json));
                response.OutputStream.Close();
                return;
            }
        }


        new public async Task<IStoredItem?> Get(string key)
        {
            var bestNode = ChordProtocol.lookup(key);
            if (bestNode == NodeName)
            {
                return await StorageBackend.Get(key);
            } else
            {
                return await base.Get(key);
            }
        }

        new public async Task<bool> Put(string key, IStoredItem value)
        {
            var bestNode = ChordProtocol.lookup(key);
            if (bestNode == NodeName)
            {
                return await StorageBackend.Put(key, value);
            } else
            {
                return await base.Put(key, value);
            }
        }

        new public async Task<bool> Remove(string key)
        {
            var bestNode = ChordProtocol.lookup(key);
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
