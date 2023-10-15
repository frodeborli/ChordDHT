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
        protected List<Route> Routes = new List<Route>();
        public IRequestHandler? NotFoundRequestHandler { get; private set; }
        public IRequestHandler? NotImplementedRequestHandler { get; private set; }
        private bool RouteListIsDirty = false;

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
            Routes.Add(route);
            RouteListIsDirty = true;
        }

        public void RemoveRoute(Route route)
        {
            Routes.Remove(route);
            RouteListIsDirty = true;
        }

        private void PrepareRouteList()
        {
            if (!RouteListIsDirty)
            {
                return;
            }

            Console.WriteLine("Sorting routes according to weight...");

            Routes.Sort((a, b) => {
                return b.Priority - a.Priority;
            });

            RouteListIsDirty = false;
        }

        public bool HandleRequest(HttpListenerContext context, RequestVariables? variables)
        {
            PrepareRouteList();

            foreach (Route route in Routes)
            {
                if (route.HandleRequest(context, variables))
                {
                    return true;
                }
            }
            return false;
        }

    }

}
