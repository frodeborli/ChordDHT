using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace ChordDHT.Util
{
    public class RequestHandler : IRequestHandler
    {
        protected Func<HttpListenerContext, RequestVariables?, bool> listener;

        public RequestHandler(Func<HttpListenerContext, RequestVariables?, bool> handlerFunction)
        {
            this.listener = handlerFunction;
        }

        public RequestHandler(Func<HttpListenerContext, RequestVariables?, Task> handlerFunction)
        {
            this.listener = (context, requestVariables) =>
            {
                Task.Run(() =>
                {
                    try
                    {
                        handlerFunction(context, requestVariables);
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"{ex.GetType()}: {ex.Message}");
                    }
                });
                return true;
            };
        }

        public bool HandleRequest(HttpListenerContext context, RequestVariables? variables)
        {
            return (this.listener)(context, variables);
        }
    }
}
