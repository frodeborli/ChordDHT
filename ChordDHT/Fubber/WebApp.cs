using ChordDHT.Fubber;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace Fubber
{
    public class WebApp
    {
        public event Func<Task>? AppStarted;
        public event Func<Task>? AppStopping;
        public Uri[] Prefixes { get; private set; }
        public Router Router { get; private set; }

        public HttpClientHandler HttpClientHandler { get; private set; }
        public HttpClient HttpClient { get; private set; }

        private CancellationTokenSource? CancellationTokenSource = null;

        private HttpListener HttpListener;

        public ILogger Logger;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="prefixes">Prefixes for the application such as 'http://hostname:80/'</param>
        public WebApp(Uri[] prefixes, ILogger logger)
        {
            Logger = logger;
            Prefixes = prefixes;

            HttpListener = new HttpListener();
            foreach (Uri prefix in prefixes)
            {
                HttpListener.Prefixes.Add(prefix.ToString());
            }

            Router = new Router(this);

            HttpClientHandler = new HttpClientHandler
            {
                AllowAutoRedirect = false,
                MaxConnectionsPerServer = 32,
            };
            HttpClient = new HttpClient(HttpClientHandler);
        }
        
        public WebApp(Uri prefix, ILogger logger) : this(new Uri[] { prefix }, logger)
        { }

        public WebApp(string prefix, ILogger logger) : this(new Uri[] { new Uri(prefix) }, logger)
        { }

        public WebApp(string[] prefixes, ILogger logger) : this(Array.ConvertAll(prefixes, p => new Uri(p)), logger)
        { }

        public async Task Run(CancellationToken cancellationToken=default)
        {
            if (this.CancellationTokenSource != null)
            {
                throw new InvalidOperationException("WebApp is already running");
            }

            CancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

            try
            {
                HttpListener.Start();
            } catch (Exception ex)
            {
                Logger.Error($"Failed to start the application:\n{ex}");
                Environment.Exit(1);
                return;
            }
            CancellationTokenSource.Token.Register(() => {
                Logger.Info("Terminating application");
            });

            if (AppStarted != null)
            {
                await InvokeAllAsync(AppStarted);
            }

            Logger.Info($"Waiting for connections on {string.Join(", ", Prefixes.Select(p => $"{p}").ToArray())}");
            while (!this.CancellationTokenSource.IsCancellationRequested)
            {
                var context = await HttpListener.GetContextAsync();
                if (context != null)
                {
                    var task = Task.Run(async () =>
                    {
                        try
                        {
                            var httpContext = new HttpContext(this, context, CancellationTokenSource.Token);

                            if (!await Router.TryHandleRequest(httpContext))
                            {
                                await httpContext.Send.NotFound();
                            }
                            if (context.Response.StatusCode != 200)
                            {
                                Logger.Info($"{context.Request.HttpMethod} {context.Request.RawUrl} {context.Response.StatusCode} {context.Response.StatusDescription}");
                            }
                        }
                        catch (Exception ex)
                        {
                            Logger.Error($"{context.Request.HttpMethod} {context.Request.RawUrl} {context.Response.StatusCode} {context.Response.StatusDescription}\n{ex}");
                        }
                        // Ensure the response is closed when we get to this point
                        context.Response.Close();
                    });
                }
            }
            if (AppStarted != null)
            {
                Logger.Debug("Running AppStopping events");
                await InvokeAllAsync(AppStopping);
                Logger.Debug("Finished running AppStopping events");
            }
            HttpListener.Stop();
        }

        private async Task InvokeAllAsync(Func<Task>? eventEmitter)
        {
            if (eventEmitter == null)
            {
                return;
            }
            var tasks = new List<Task>();
            foreach (Func<Task> handler in eventEmitter.GetInvocationList().Cast<Func<Task>>())
            {
                tasks.Add(handler());
            }
            await Task.WhenAll(tasks);
        }
    }
}
