using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace ChordDHT.Util
{
    public class Route : IRequestHandler
    {
        protected string method;
        protected Regex pattern;
        protected RequestHandler requestHandler;

        /**
         * Construct a route by providing a method and a regex pattern. The handler function
         * will receive the HttpListenerContext and must return true if the route is being
         * handled by the handler delegate, and false if the route will not be handled by it.
         * 
         * Example Regex Pattern: @"^/users/(?<id>\d+)$"
         */
        public Route(string method, string pattern, Func<HttpListenerContext, bool> handler)
        {
            this.method = method.ToUpper();
            this.pattern = new Regex(pattern);
            this.requestHandler = new RequestHandler(handler);
        }

        /**
         * Construct a route by providing a method and a regex pattern. The handler delegate
         * will receive the HttpListenerContext and a GroupCollection (containing the matches
         * from the regex pattern) and must return true if the route is being handled by the
         * handler delegate, and false if the route will not be handled by it.
         * 
         * Example Regex Pattern: @"^/users/(?<id>\d+)$"
         */
        public Route(string method, string pattern, Func<HttpListenerContext, RequestVariables, bool> handler)
        {
            this.method = method.ToUpper();
            this.pattern = new Regex(pattern);
            this.requestHandler = new RequestHandler(handler);
        }

        /**
         * Construct a route by providing a method and a regex pattern. The handler delegate
         * must return an object or the null value which will be serialized using JsonSerializer
         * and sent as a response to the request.
         * 
         * Example Regex Pattern: @"^/users/(?<id>\d+)$"
         */
        public Route(string method, string pattern, Func<HttpListenerContext, object?> handler)
        {
            this.method = method.ToUpper();
            this.pattern = new Regex(pattern);
            this.requestHandler = new RequestHandler(handler);
        }

        /**
         * Construct a route by providing a method and a regex pattern. The handler delegate
         * must return an object or the null value which will be serialized using JsonSerializer
         * and sent as a response to the request.
         * 
         * Example Regex Pattern: @"^/users/(?<id>\d+)$"
         */
        public Route(string method, string pattern, Func<HttpListenerContext, RequestVariables, object?> handler)
        {
            this.method = method.ToUpper();
            this.pattern = new Regex(pattern);
            this.requestHandler = new RequestHandler(handler);
        }

        public bool handleRequest(HttpListenerContext context, RequestVariables variables)
        {
            if (context.Request.Url == null)
            {
                return false;
            }
            var match = this.pattern.Match(context.Request.Url.AbsolutePath);
            if (!match.Success)
            {
                return false;
            }
            var vars = new RequestVariables();
            
            foreach (string groupName in this.pattern.GetGroupNames())
            {
                if (int.TryParse(groupName, out _)) continue; // Skip numeric group names
                vars[groupName] = match.Groups[groupName].Value;
            }

            return this.requestHandler.handleRequest(context, vars);
        }

    }
}
