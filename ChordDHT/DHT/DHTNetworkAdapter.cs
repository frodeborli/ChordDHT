using ChordDHT.ChordProtocol;
using ChordDHT.ChordProtocol.Exceptions;
using ChordProtocol;
using Fubber;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
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
        private Dictionary<Type, IMessageHandler> MessageHandlers;                                 

        public DHTNetworkAdapter(WebApp webApp)
        {
            WebApp = webApp;
            Logger = Dev.Logger("DHTNetworkAdapter");
            Route = new Route("PUT", "/chord-node-api", RequestReceivedHandler);
            MessageHandlers = new Dictionary<Type, IMessageHandler>();
            WebApp.AppStarted += StartAsync;
            WebApp.AppStopping += StopAsync;
        }

        public async Task<TResponseMessage> SendMessageAsync<TRequestMessage, TResponseMessage>(TRequestMessage message)
            where TRequestMessage : IMessage
            where TResponseMessage : IMessage
        {
            var targetUrl = $"http://{message.Receiver.Name}/chord-node-api";

            var requestBody = new StringContent(message.ToJson(), Encoding.UTF8, "application/json");

            // Must follow redirects
            HttpResponseMessage? response = null;

            do
            {
                try
                {
                    response = await WebApp.HttpClient.PutAsync(targetUrl, requestBody);
                } catch (HttpRequestException ex)
                {
                    // node seems to be gone or down
                    var goneNodeName = new Uri(targetUrl).Authority;
                    throw new NodeGoneException(goneNodeName);
                }

                if (response.StatusCode == HttpStatusCode.Redirect)
                {
                    Logger.Debug($"Following redirect from {targetUrl} to {response.Headers.Location.ToString()}");
                    targetUrl = response.Headers.Location.ToString();                    
                } else if (response.StatusCode == HttpStatusCode.InternalServerError)
                {
                    throw new RequestFailedException(await response.Content.ReadAsStringAsync());
                } else if (!response.IsSuccessStatusCode)
                {
                    throw new InvalidOperationException($"SOMETHING FISHY HAPPN {(int)response.StatusCode}!");
                }
            } while (!response.IsSuccessStatusCode);
            
            string responseData = await response.Content.ReadAsStringAsync();

            var responseMessage = Message.FromJson(responseData);
            if (responseMessage is TResponseMessage typedResponseMessage)
            {
                return typedResponseMessage;
            }
            throw new InvalidDataException("The received response was not of the correct type");
        }

        public void AddMessageHandler<TRequestMessage, TResponseMessage>(MessageHandler<TRequestMessage, TResponseMessage> handler)
            where TRequestMessage : IMessage
            where TResponseMessage : IMessage
        {
            if (MessageHandlers.ContainsKey(typeof(TRequestMessage)))
            {
                throw new InvalidOperationException($"Already have a request handler for requests of type {typeof(TRequestMessage)} returning {typeof(TResponseMessage)}");
            }

            MessageHandlers[typeof(TRequestMessage)] = handler;
        }

        public void AddMessageHandler<TRequestMessage, TResponseMessage>(Func<TRequestMessage, Task<TResponseMessage>> handler)
            where TRequestMessage : IMessage
            where TResponseMessage : IMessage
            => AddMessageHandler(new MessageHandler<TRequestMessage, TResponseMessage>(handler));


        private async Task RequestReceivedHandler(HttpContext context)
        {
            // read the message body
            StreamReader reader = new StreamReader(context.Request.InputStream);
            var requestBody = await reader.ReadToEndAsync();

            var receivedMessage = Message.FromJson(requestBody);

            if (receivedMessage == null)
            {
                Logger.Error($"Received a message which was deserialized to null:\n{requestBody}");
                await context.Send.BadRequest("Unable to parse message body");
                return;
            }

            if (!MessageHandlers.ContainsKey(receivedMessage.GetType()))
            {
                Logger.Error($"Received unsupported message type {receivedMessage.GetType().Name}");
                await context.Send.BadRequest("Unsupported message type");
                return;
            }

            var handler = MessageHandlers[receivedMessage.GetType()];

            try
            {
                var responseMessage = await handler.HandleMessageAsync(receivedMessage);
                responseMessage.Sender = receivedMessage.Receiver;
                responseMessage.Receiver = receivedMessage.Sender;
                await context.Send.Ok(responseMessage.ToJson(), "application/json");
            } catch (RequestFailedException ex)
            {
                await context.Send.InternalServerError(ex.Message);
            } catch (RedirectException ex)
            {
                // Redirect to a different endpoint
                Uri thisUrl = context.Request.Url!;
                string newUrl = $"{thisUrl.Scheme}://{ex.TargetNode}{thisUrl.PathAndQuery}";
                await context.Send.TemporaryRedirect(newUrl);
            }
            return;
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
