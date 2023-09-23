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
        public readonly string Method;
        public readonly Regex Pattern;
        protected IRequestHandler RequestHandler;

        public Route(string method, Regex pattern, IRequestHandler requestHandler)
        {
            this.Method = method.ToUpper();
            this.Pattern = pattern;
            this.RequestHandler = requestHandler;
        }

        public Route(string method, string pattern, IRequestHandler requestHandler)
            : this(method, new Regex(pattern), requestHandler) { }


        public bool HandleRequest(HttpListenerContext context, RequestVariables? variables)
        {
            if (context.Request.HttpMethod.ToUpper() != this.Method.ToUpper()) {
                return false;
            }
            if (context.Request.Url == null)
            {
                return false;
            }
            var match = this.Pattern.Match(context.Request.Url.AbsolutePath);
            if (!match.Success)
            {
                return false;
            }
            var vars = new RequestVariables();
            
            foreach (string groupName in this.Pattern.GetGroupNames())
            {
                if (int.TryParse(groupName, out _)) continue; // Skip numeric group names
                vars[groupName] = match.Groups[groupName].Value;
            }

            return this.RequestHandler.HandleRequest(context, vars);
        }

        public override string ToString()
        {
            return $"Route({Method} {Pattern}";
        }
    }
}
