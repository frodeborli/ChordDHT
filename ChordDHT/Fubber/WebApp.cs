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
        public Uri[] Prefixes { get; private set; }
        public Router Router { get; private set; }

        public HttpClientHandler HttpClientHandler { get; private set; }
        public HttpClient HttpClient { get; private set; }

        private CancellationTokenSource? CancellationTokenSource = null;

        private HttpListener HttpListener;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="prefixes">Prefixes for the application such as 'http://hostname:80/'</param>
        public WebApp(Uri[] prefixes)
        {
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

            Dev.Info("Launching application");

            CancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

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
                            if (System.Diagnostics.Debugger.IsAttached)
                            {
                                System.Diagnostics.Debugger.Break();
                            }
                            else
                            {
                                Dev.Error($"{context.Request.HttpMethod} {context.Request.RawUrl} {context.Response.StatusCode} {context.Response.StatusDescription}\n{ex}");
                            }
                        }
                        // Ensure the response is closed when we get to this point
                        context.Response.Close();
                    });
                }
            }
        }
    }
}
