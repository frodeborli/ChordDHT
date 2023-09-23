using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace ChordDHT.Util
{
    public class Router : IRequestHandler
    {
        protected List<Route> routes = new List<Route>();
        public IRequestHandler? NotFoundRequestHandler { get; private set; }
        public IRequestHandler? NotImplementedRequestHandler { get; private set; }

        public Router(IRequestHandler? notFoundHandler = null, IRequestHandler? notImplementedRequestHandler = null)
        {
            NotFoundRequestHandler = notFoundHandler;
            NotImplementedRequestHandler = notImplementedRequestHandler;
        }

        public void SendPageNotFound(HttpListenerContext context)
        {
            if (NotFoundRequestHandler != null)
            {
                NotFoundRequestHandler.HandleRequest(context);
            } else
            {
                (new GenericStatusRequestHandler(HttpStatusCode.NotFound, "Not found")).HandleRequest(context);
            }
        }

        public void SendNotImplemented(HttpListenerContext context)
        {
            if (NotImplementedRequestHandler != null)
            {
                NotImplementedRequestHandler.HandleRequest(context);
            } else
            {
                (new GenericStatusRequestHandler(HttpStatusCode.NotImplemented, "Not implemented")).HandleRequest(context);
            }
        }

        public void AddRoute(Route route)
        {
            routes.Add(route);
        }

        public bool HandleRequest(HttpListenerContext context, RequestVariables? variables)
        {
            foreach (Route route in routes)
            {
                Console.WriteLine($"Trying route '{route}' for request to '{context.Request.Url}'");
                if (route.HandleRequest(context, variables))
                {
                    return true;
                }
            }
            return false;
        }

    }

}
