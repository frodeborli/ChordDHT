using ChordProtocol;
using Fubber;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace ChordDHT.DHT
{
    class DHTNetworkAdapter : IChordNetworkAdapter
    {
        private WebApp WebApp;
        private Dev.LoggerContext Logger;
        private Route Route;

        public Func<Message, Task<Message>>? RequestHandler { get; set; } = null;

        public DHTNetworkAdapter(WebApp webApp)
        {
            WebApp = webApp;
            Logger = Dev.Logger("DHTNetworkAdapter");
            Route = new Route("PUT", "/chord-node-api", RequestReceivedHandler);
            WebApp.AppStarted += StartAsync;
            WebApp.AppStopping += StopAsync;
        }

        public void SetRequestHandler(Func<Message, Task<Message>> handler)
        {
            RequestHandler = handler;
        }

        private async Task RequestReceivedHandler(HttpContext context)
        {
            Logger.Debug($"Received a request to {context.Request.Url}");

            if (RequestHandler == null)
            {
                await context.Send.InternalServerError("Request handler not set");
                return;
            }

            // read the message body
            using (StreamReader reader = new StreamReader(context.Request.InputStream))
            {
                var requestBody = await reader.ReadToEndAsync();
                Dev.Debug($"DESERIALIZE REQUEST_BODY\n{requestBody}");
                var d = JsonSerializer.Deserialize<JsonElement>(requestBody);

                var requestMessage = JsonSerializer.Deserialize<Message>(requestBody);
                if (requestMessage == null)
                {
                    await context.Send.BadRequest("Unable to parse request body");
                    return;
                }
                var responseMessage = await RequestHandler(requestMessage);
                Dev.Dump(responseMessage);
                var responseBody = JsonSerializer.Serialize(responseMessage);

                Logger.Debug($"SENDING RESPONSE BODY\n'{responseBody}'");

                await context.Send.Ok(responseBody);
                return;
            }
        }

        public async Task<Message> SendMessageAsync(Node receiver, Message request)
        {
            var targetUrl = $"http://{receiver.Name}/chord-node-api";
            Logger.Debug($"Sending a message to {request.Source} at {targetUrl}");

            var requestBody = JsonSerializer.Serialize(request);
            Dev.Info($"FUCKING SERIALIZED VALUE:\n{requestBody}");
            Dev.Dump(request);
            var response = await WebApp.HttpClient.PutAsync(targetUrl, new StringContent(requestBody, Encoding.UTF8, "application/json"));

            if (!response.IsSuccessStatusCode)
            {
                throw new InvalidDataException($"Received an invalid response to request for {targetUrl}");
            }

            string responseData = await response.Content.ReadAsStringAsync();

            Dev.Debug($"DESERIALIZE REQUEST_RESPONSE\n'{responseData}'");
            Dev.Dump(responseData);
            await Task.Delay(5000);
            var responseMessage = JsonSerializer.Deserialize<Message>(responseData);
            if (responseMessage == null)
            {
                throw new InvalidDataException("Received unparseable response");
            }
            return responseMessage;
        }

        public Task StartAsync()
        {
            Logger.Debug("Starting");
            WebApp.Router.AddRoute(Route);
            return Task.CompletedTask;
        }

        public Task StopAsync()
        {
            Logger.Debug("Stopping");
            WebApp.Router.RemoveRoute(Route);
            return Task.CompletedTask;
        }

    }
}
