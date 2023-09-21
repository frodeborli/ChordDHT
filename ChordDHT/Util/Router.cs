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

        public void addRoute(Route route)
        {
            routes.Add(route);
        }

        public bool handleRequest(HttpListenerContext context, RequestVariables variables)
        {
            foreach (Route route in routes)
            {
                if (route.handleRequest(context, variables))
                {
                    return true;
                }
            }
            return false;
        }

    }

}
