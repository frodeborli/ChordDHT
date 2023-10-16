using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace ChordDHT.Util
{
    public class Route : IHttpRouter.Route
    {
        public RequestDelegate Handler { get; }
        public int Priority { get; }
        private Predicate<HttpContext> _Predicate;
        private string _AsString;

        public Route(Predicate<HttpContext> predicate, RequestDelegate handler, int priority = 0)
        {
            Handler = handler;
            Priority = priority;
            _AsString = "Route(CustomPredicateFunction)";
            _Predicate = predicate;
        }

        public Route(Func<Predicate<HttpContext>> predicateBuilder, RequestDelegate handler, int priority = 0)
            : this(predicateBuilder(), handler, priority) { }

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
                                    context.Variables[item] = match.Groups[item].Value;
                                }
                            }
                            return true;
                        }

                    }
                    return false;
                };
            }, handler, priority)
        { }

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
}
