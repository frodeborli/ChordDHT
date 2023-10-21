using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace Fubber
{
    public class Router {
        private List<Route> Routes = new List<Route>();
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
            await handler(context);
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

        public void AddRoute(Route route)
        {
            Routes.Add(route);
            RouteListNeedsSorting = true;
        }

        public void AddRoute(IEnumerable<Route> routes)
        {
            foreach (var route in routes)
            {
                AddRoute(route);
            }
        }

        public void RemoveRoute(Route route)
        {
            Routes.Remove(route);
            RouteListNeedsSorting = true;
        }

        public void RemoveRoute(IEnumerable<Route> routes)
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

        public Route? GetRouteFor(HttpContext context)
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

    public class Route
    {
        public RequestDelegate Handler { get; }
        public int Priority { get; }
        private Predicate<HttpContext> _Predicate;
        private string _AsString;

        public Route(Predicate<HttpContext> predicate, RequestDelegate handler, int priority = 0)
        {
            Handler = handler;
            Priority = priority;
            _AsString = "Route(Predicate)";
            _Predicate = predicate;
        }

        public Route(Func<Predicate<HttpContext>> predicateBuilder, RequestDelegate handler, int priority = 0)
            : this(predicateBuilder(), handler, priority)
        {
            _AsString = "Route(PredicateBuilder)";
        }

        public Route(IEnumerable<string> methods, Regex pattern, RequestDelegate handler, int priority = 0)
            : this(() => {
                var _methods = new HashSet<string>();
                foreach (var method in methods)
                {
                    _methods.Add(method.ToUpper());
                }
                return (context) =>
                {
                    if (context.Request.RawUrl == null)
                    {
                        return false;
                    }
                    if (_methods.Contains(context.Request.HttpMethod.ToUpper()))
                    {
                        var match = pattern.Match(context.Request.RawUrl);
                        if (match.Success && match.Length == context.Request.RawUrl.Length)
                        {
                            foreach (var item in pattern.GetGroupNames())
                            {
                                if (item != null)
                                {
                                    context.RouteVariables[item] = match.Groups[item].Value;
                                }
                            }
                            return true;
                        }

                    }
                    return false;
                };
            }, handler, priority)
        {
            _AsString = $"Route({string.Join("|", methods)} {pattern})";
        }

        public Route(string method, Regex pattern, RequestDelegate handler, int priority = 0)
            : this(method.Split('|'), pattern, handler, priority) { }

        public Route(IEnumerable<string> methods, string pattern, RequestDelegate handler, int priority = 0)
            : this(methods, new Regex(pattern), handler, priority) { }
        public Route(string method, string pattern, RequestDelegate handler, int priority = 0)
            : this(method, new Regex(pattern), handler, priority) { }

        public bool Predicate(HttpContext context)
        {
            return _Predicate(context);
        }

        public override string ToString()
        {
            return _AsString;
        }
    }

    public class RouteVariables : Dictionary<string, string>
    {
    }

    public delegate Task RequestDelegate(HttpContext context);
}
