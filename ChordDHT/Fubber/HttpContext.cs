using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace Fubber
{
    public class HttpContext
    {
        public WebApp WebApp { get; }
        public HttpListenerRequest Request { get; }
        public HttpListenerResponse Response { get; }
        public RouteVariables RouteVariables { get; }

        public ContextSender Send { get; }

        public CancellationToken RequestAborted { get; }

        private CancellationTokenSource CancellationTokenSource;

        public HttpContext(WebApp webApp, HttpListenerContext context, CancellationToken appCancellationToken)
        {
            WebApp = webApp;
            Request = context.Request;
            Response = context.Response;
            RouteVariables = new RouteVariables();
            Send = new ContextSender(this);

            CancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(appCancellationToken);
            RequestAborted = CancellationTokenSource.Token;
        }


    }
}
