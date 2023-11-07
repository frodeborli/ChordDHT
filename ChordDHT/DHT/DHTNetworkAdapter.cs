using ChordDHT.ChordProtocol;
using ChordDHT.ChordProtocol.Exceptions;
using ChordDHT.ChordProtocol.Messages;
using ChordDHT.Fubber;
using ChordProtocol;
using Fubber;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Linq;
using System.Net;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace ChordDHT.DHT
{
    class DHTNetworkAdapter : INetworkAdapter
    {
        private DHTServer App;
        private ILogger Logger;
        private Route Route;
        private Dictionary<Type, IGenericRequestHandler> MessageHandlers;
        private Chord? Chord = null;

        public DHTNetworkAdapter(DHTServer webApp, ILogger logger)
        {
            App = webApp;
            Logger = logger;
            Route = new Route("PUT", "/chord-node-api", RequestReceivedHandler);
            MessageHandlers = new Dictionary<Type, IGenericRequestHandler>();
            App.AppStarted += StartAsync;
            App.AppStopping += StopAsync;
        }

        public void SetChord(Chord chord)
        {
            Chord = chord;
        }

        private string GetTargetUrl(Node node)
        {
            return $"http://{node.Name}/chord-node-api";
        }

        public async Task<TResponse> SendMessageAsync<TResponse>(Node receiver, IRequest<TResponse> message)
            where TResponse : IResponse
        {
            if (receiver == null)
            {
                throw new NullReferenceException(nameof(receiver));
            }

            var messageType = message.GetType().Name;

            var requestBody = new StringContent(Util.ToTypedJSON(message), Encoding.UTF8, "application/json");

            // Must follow redirects
            HttpResponseMessage? response = null;

            var targetUrl = GetTargetUrl(receiver);
            try
            {
                var cts = new CancellationTokenSource();
                cts.CancelAfter(TimeSpan.FromSeconds(30));
                var sw = Stopwatch.StartNew();
                response = await App.HttpClient.PutAsync(targetUrl, requestBody, cts.Token);
                sw.Stop();
                if (sw.Elapsed > TimeSpan.FromMilliseconds(200))
                {
                    Logger.Debug($"{message.GetType().Name} @ {targetUrl} took {sw.Elapsed}");
                }
            }
            catch (OperationCanceledException)
            {
                Logger.Warn($" - timeout for {message}");
                throw new NetworkException(receiver, "Request timed out");
            } 
            catch (HttpRequestException ex)
            {
                Logger.Warn($" - network error for {message}");
                // node seems to be gone or down
                throw new NetworkException(receiver, ex.ToString());
            }

            if (!response.IsSuccessStatusCode)
            {
                var errorMessage = await response.Content.ReadAsStringAsync();
                if ((int)response.StatusCode == 409)
                {
                    Logger.Warn($" - rejected {message}\n   {errorMessage}");
                    throw new RejectedException(errorMessage);
                }
                else
                {
                    Logger.Warn($" - failed {message}\n   statusCode={response.StatusCode}\n");
                    throw new NetworkException(receiver, $"Got error: {errorMessage}");
                }
            }
            
            string responseData = await response.Content.ReadAsStringAsync();

            var responseMessage = Util.FromTypedJSON(responseData);

            if (responseMessage == null)
            {
                Logger.Warn($" - NULL response for {message}");
                throw new NetworkException(receiver, $"Failed to parse response: {responseData}");
            }

            if (!(responseMessage is TResponse responseMessageTyped))
            {
                Logger.Warn($" - invalid response for {message}");
                throw new InvalidDataException($"The received response was not of the correct type: {responseMessage.GetType()}");
            }

            return responseMessageTyped;
        }

        public void AddHandler<TRequest, TResponse>(IRequestHandler<TRequest, TResponse> handler)
            where TRequest : IRequest<TResponse>
            where TResponse : IResponse
        {
            if (MessageHandlers.ContainsKey(typeof(TRequest)))
            {
                throw new InvalidOperationException($"Already have a request handler for requests of type {typeof(TRequest)} returning {typeof(TResponse)}");
            }

            MessageHandlers[typeof(TRequest)] = (IGenericRequestHandler)handler;
        }

        public void AddHandler<TRequest, TResponse>(Func<TRequest, Task<TResponse>> handler)
            where TRequest : IRequest<TResponse>
            where TResponse : IResponse => AddHandler(new RequestHandler<TRequest, TResponse>(handler));

        private async Task RequestReceivedHandler(HttpContext context)
        {
            // read the message body
            StreamReader reader = new StreamReader(context.Request.InputStream);
            var requestBody = await reader.ReadToEndAsync();

            var receivedMessageObject = Util.FromTypedJSON(requestBody);

            if (receivedMessageObject == null)
            {
                Logger.Error($"Received a message which was deserialized to null:\n{requestBody}");
                await context.Send.BadRequest("Unable to parse message body");
                return;
            }

            if (!(receivedMessageObject is IGenericRequest receivedMessage))
            {
                Logger.Error($"Received a message which was not an IGenericRequest object:\n{requestBody}");
                await context.Send.BadRequest("Illegal message type");
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
                var sw = new Stopwatch();
                sw.Start();
                var responseMessage = await handler.HandleMessageAsync(receivedMessage);
                sw.Stop();
                if (sw.ElapsedMilliseconds > 50)
                {
                    Logger.Warn($"Spent {sw.ElapsedMilliseconds} ms in handler for {receivedMessage.GetType().Name}");
                }
                await context.Send.Ok(Util.ToTypedJSON(responseMessage), "application/json");
            }
            catch (RejectedException ex)
            {
                await context.Send.Conflict(ex.Message);
            }
            catch (Exception ex)
            {
                await context.Send.InternalServerError(ex.Message);
            }
            return;
        }

        public Task StartAsync()
        {
            App.Router.AddRoute(Route);
            return Task.CompletedTask;
        }

        public Task StopAsync()
        {
            App.Router.RemoveRoute(Route);
            return Task.CompletedTask;
        }
    }
}
