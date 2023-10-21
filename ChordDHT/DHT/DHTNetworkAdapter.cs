using ChordProtocol;
using Fubber;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ChordDHT.DHT
{
    class DHTNetworkAdapter : IChordNetworkAdapter
    {
        public event EventHandler<Message>? MessageReceived;
        private WebApp WebApp;
        private Dev.LoggerContext Logger;
        private Route Route;

        public DHTNetworkAdapter(WebApp webApp)
        {
            WebApp = webApp;
            Logger = Dev.Logger("DHTNetworkAdapter");
            Route = new Route("GET", "/chord-node-api", RequestHandler);
        }

        private async Task RequestHandler(HttpContext context)
        {
            Logger.Debug($"Received a request to {context.Request.Url}");
            await context.Send.Ok("Hei sveis");
        }

        public async Task<Message> SendMessageAsync(Node sender, Node receiver, Message request)
        {
            var targetUrl = $"http://{receiver.Name}/chord-node-api";
            Logger.Debug($"Sending a message to {sender} at {targetUrl}");
            var httpResult = await WebApp.HttpClient.GetAsync(targetUrl);
            Dev.Debug("Received response from node:");
            Dev.Dump(httpResult);
            var response = new Message(receiver, "example-response", new(string, object?)[]
            {
                ("key", "value")
            });
            return response;
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
