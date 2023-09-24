using ChordDHT.ChordProtocol;
using System.Net;
using System.Text;
using System.Text.Json;

namespace ChordDHT.DHT
{
    public class DHTClient : IStorageBackend
    {
        protected Chord ChordProtocol;
        private HttpClientHandler HttpClientHandler;
        protected HttpClient HttpClient;
        protected readonly string Prefix;
        public string NodeName;

        public int LastRequestHops { get; private set; } = 0;

        public DHTClient(string nodeName, string prefix="/")
        {
            this.NodeName = nodeName;
            this.Prefix = prefix;
            HttpClientHandler = new HttpClientHandler
            {
                AllowAutoRedirect = false
            };
            HttpClient = new HttpClient(HttpClientHandler);
            HttpClient.DefaultRequestHeaders.UserAgent.ParseAdd(nodeName.Replace(":", "/"));
            ChordProtocol = new Chord(nodeName);
        }

        public async Task<IStoredItem?> Get(string key)
        {
            LastRequestHops = 0;
            var bestNode = ChordProtocol.Lookup(key);

            var nextUrl = $"http://{bestNode}{Prefix}{key}";

            // Find the node responsible for this key
            for (; ; )
            {
                LastRequestHops++;
                var response = await HttpClient.GetAsync(nextUrl);

                if (response.IsSuccessStatusCode)
                {
                    // We got the data we wanted
                    var jsonString = await response.Content.ReadAsStringAsync();
                    return JsonSerializer.Deserialize<IStoredItem>(jsonString);
                }
                else if (response.StatusCode == HttpStatusCode.NotFound)
                {
                    // The node that was supposed to have the key does not have it
                    return null;
                }
                else if (response.StatusCode == HttpStatusCode.RedirectKeepVerb && response.Headers?.Location != null)
                {
                    // The request should be repeated at another node
                    nextUrl = response.Headers.Location?.ToString();
                } else
                {
                    throw new InvalidOperationException($"Invalid response from node at {nextUrl} (statusCode={response.StatusCode})");
                }
            }
        }

        public async Task<bool> ContainsKey(string key)
        {
            return await Get(key) == null;
        }

        public async Task<bool> Put(string key, IStoredItem value)
        {
            LastRequestHops = 0;

            var requestBody = new ByteArrayContent(value.Data);
            requestBody.Headers.ContentType = new System.Net.Http.Headers.MediaTypeHeaderValue(value.ContentType);

            var bestNode = ChordProtocol.Lookup(key);
            var nextUrl = $"http://{bestNode}/storage/{key}";

            // Find the node responsible for this key
            for (; ; )
            {
                LastRequestHops++;
                var response = await HttpClient.PutAsync(nextUrl, requestBody);

                if (response.IsSuccessStatusCode)
                {
                    // This node was willing to save our data
                    return true;
                }
                else if (response.StatusCode == HttpStatusCode.RedirectKeepVerb && response.Headers?.Location != null)
                {
                    // This node forwards us to another node for saving the data
                    nextUrl = response.Headers.Location?.ToString();
                }
                else
                {
                    throw new InvalidOperationException($"Invalid response from node at {nextUrl} (statusCode={response.StatusCode})");
                }
            }
        }

        public async Task<bool> Remove(string key)
        {
            LastRequestHops = 0;

            var bestNode = ChordProtocol.Lookup(key);
            var nextUrl = $"http://{bestNode}/storage/{key}";

            // Find the node responsible for this key
            for (; ; )
            {
                LastRequestHops++;
                var response = await HttpClient.DeleteAsync(nextUrl);

                if (response.IsSuccessStatusCode)
                {
                    // We found the node responsible for the key and the key was deleted
                    return true;
                }
                else if (response.StatusCode == HttpStatusCode.NotFound)
                {
                    // The node that was supposed to have the key does not have it
                    return false;
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
