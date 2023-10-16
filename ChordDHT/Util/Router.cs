using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace ChordDHT.Util
{
    public class Router : IHttpRouter
    {
        private List<IHttpRouter.Route> Routes = new List<IHttpRouter.Route>();
        private bool RouteListNeedsSorting = false;
        private WebApp WebApp;

        public Router(WebApp webApp)
        {
            WebApp = webApp;
        }

        public async Task<bool> TryHandleRequest(HttpContext context)
        {
            var handler = GetHandlerFor(context);
            if (handler == null)
            {
                return false;
            }
            Console.WriteLine("Awaiting handler");
            await handler(context);
            Console.WriteLine("Handler done");
            return true;
        }

        public RequestDelegate RequestHandler {
            get {
                return async (HttpContext context) =>
                {
                    var handler = this.GetHandlerFor(context);
                    if (handler != null)
                    {
                        await handler(context);
                    } else
                    {
                        await context.Send.NotFound();
                    }
                };
            }
        }

        public void AddRoute(IHttpRouter.Route route)
        {
            Routes.Add(route);
            RouteListNeedsSorting = true;
        }

        public void AddRoute(IEnumerable<IHttpRouter.Route> routes)
        {
            foreach (var route in routes)
            {
                AddRoute(route);
            }
        }

        public void RemoveRoute(IHttpRouter.Route route)
        {
            Routes.Remove(route);
            RouteListNeedsSorting = true;
        }

        public void RemoveRoute(IEnumerable<IHttpRouter.Route> routes)
        { 
            foreach (var route in routes)
            {
                RemoveRoute(route);
            }
            RouteListNeedsSorting = true;
        }

        private void SortRoutes()
        {
            if (!RouteListNeedsSorting)
            {
                return;
            }

            Routes.Sort((a, b) => {
                return b.Priority - a.Priority;
            });

            RouteListNeedsSorting = false;
        }

        public IHttpRouter.Route? GetRouteFor(HttpContext context)
        {
            SortRoutes();

            foreach (var route in Routes)
            {
                if (route.Predicate(context))
                {
                    return route;
                }
            }

            return default;
        }

        public RequestDelegate? GetHandlerFor(HttpContext context)
        {
            var route = GetRouteFor(context);
            if (route == null)
            {
                return null;
            }
            return route.Handler;
        }

    }

}
