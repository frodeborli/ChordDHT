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
        public event Func<Task> AppStarted;
        public event Func<Task> AppStopping;
        public Uri[] Prefixes { get; private set; }
        public Router Router { get; private set; }

        public HttpClientHandler HttpClientHandler { get; private set; }
        public HttpClient HttpClient { get; private set; }

        private CancellationTokenSource? CancellationTokenSource = null;

        private HttpListener HttpListener;

        private Dev.LoggerContext Logger;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="prefixes">Prefixes for the application such as 'http://hostname:80/'</param>
        public WebApp(Uri[] prefixes)
        {
            Logger = Dev.Logger("WebApp");
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
        
        public WebApp(Uri prefix) : this(new Uri[] { prefix })
        { }

        public WebApp(string prefix) : this(new Uri[] { new Uri(prefix) })
        { }

        public WebApp(string[] prefixes) : this(Array.ConvertAll(prefixes, p => new Uri(p)))
        { }

        public async Task Run(CancellationToken cancellationToken=default)
        {
            if (this.CancellationTokenSource != null)
            {
                throw new InvalidOperationException("WebApp is already running");
            }

            Logger.Info("Launching application");

            CancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

            HttpListener.Start();
            CancellationTokenSource.Token.Register(() => {
                Logger.Info("Terminating application");
            });

            if (AppStarted != null)
            {
                Logger.Debug("Running AppStarted events");
                await InvokeAllAsync(AppStarted);
                Logger.Debug("Finished running AppStarted events");
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
                            Dev.Info($"{context.Request.HttpMethod} {context.Request.RawUrl} {context.Response.StatusCode} {context.Response.StatusDescription}");
                        }
                        catch (Exception ex)
                        {
                            Dev.Error($"{context.Request.HttpMethod} {context.Request.RawUrl} {context.Response.StatusCode} {context.Response.StatusDescription}\n{ex}");
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
