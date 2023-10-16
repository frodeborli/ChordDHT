using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace ChordDHT.Util
{
    public class WebApp
    {
        public Router Router { get; private set; }
        private CancellationTokenSource? CancellationTokenSource = null;

        public WebApp(
            Router? router = null
            )
        {
            Router = router ?? new Router(this);
        }

        public async Task Run(HttpListener httpListener, CancellationToken cancellationToken=default)
        {
            if (this.CancellationTokenSource != null)
            {
                throw new InvalidOperationException("WebApp is already running");
            }

            this.CancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

            while (!this.CancellationTokenSource.IsCancellationRequested)
            {
                var context = await httpListener.GetContextAsync();
                Console.WriteLine($"Received a request to {context.Request.RawUrl}");
                if (context != null)
                {
                    var task = Task.Run(async () =>
                    {
                        try
                        {
                            Console.WriteLine("Creating HttpContext");
                            var httpContext = new HttpContext(this, context, CancellationTokenSource.Token);

                            Console.WriteLine("TryHandleRequest");
                            if (!await Router.TryHandleRequest(httpContext))
                            {
                                Console.WriteLine("TryHandleRequest failed, sending NotFound");
                                await httpContext.Send.NotFound();
                            } else
                            {
                                Console.WriteLine("Response should have been sent now");
                            }
                        } catch (Exception ex)
                        {
                            if (System.Diagnostics.Debugger.IsAttached)
                            {
                                System.Diagnostics.Debugger.Break();
                            }
                            else
                            {
                                Console.WriteLine(ex);
                            }
                        }
                        // Ensure the response is closed when we get to this point
                        context.Response.Close();
                    });


                }
            }
        }

        

        public void RespondJson(HttpContext context, object? data)
        {
            var response = context.Response;
            response.ContentType = "application/json";
            string json = JsonSerializer.Serialize(data);
            response.OutputStream.Write(Encoding.UTF8.GetBytes(json));
            response.OutputStream.Close();
        }
    }
}
