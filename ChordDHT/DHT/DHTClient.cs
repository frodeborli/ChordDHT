using ChordDHT.ChordProtocol;
using System.Net;
using System.Text;
using System.Text.Json;

namespace ChordDHT.DHT
{

    /**
     * A ChordDHT client implementation
     */
    public class DHTClient : IStorageBackend
    {
        private HttpClientHandler HttpClientHandler;
        protected HttpClient HttpClient;
        protected readonly string Prefix;
        public string NodeName;

        public DHTClient(string primaryHostName, string prefix="/")
        {
            NodeName = primaryHostName;
            Prefix = prefix;
            HttpClientHandler = new HttpClientHandler
            {
                AllowAutoRedirect = false
            };
            HttpClient = new HttpClient(HttpClientHandler);
            HttpClient.DefaultRequestHeaders.UserAgent.ParseAdd(primaryHostName.Replace(":", "/"));
        }

        public async Task<IStoredItem?> Get(string key)
        {
            var url = $"http://{NodeName}{Prefix}{key}";
            var response = await HttpClient.GetAsync(url);

            if (response.IsSuccessStatusCode)
            {
                return await StoredItem.CreateFrom(response);
            }
            else if (response.StatusCode == HttpStatusCode.NotFound)
            {
                // The node that was supposed to have the key does not have it
                return null;
            } else
            {
                throw new InvalidOperationException($"Unexpected response from GET {url} (statusCode={response.StatusCode})");
            }
        }

        public async Task<bool> ContainsKey(string key)
        {
            return await Get(key) == null;
        }

        public async Task<bool> Put(string key, IStoredItem value)
        {
            var requestBody = new ByteArrayContent(value.Data);
            requestBody.Headers.ContentType = new System.Net.Http.Headers.MediaTypeHeaderValue(value.ContentType);

            var url = $"http://{NodeName}{Prefix}{key}";

            var response = await HttpClient.PutAsync(url, requestBody);

            if (response.IsSuccessStatusCode)
            {
                // This node was willing to save our data
                return true;
            }
            else
            {
                throw new InvalidOperationException($"Unexpected response from PUT {url} (statusCode={response.StatusCode})");
            }
        }

        public async Task<bool> Remove(string key)
        {
            var url = $"http://{NodeName}{Prefix}{key}";

            var response = await HttpClient.DeleteAsync(url);

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
            else
            {
                throw new InvalidOperationException($"Unexpected response from DELETE {url} (statusCode={response.StatusCode})");
            }
        }
    }
}
