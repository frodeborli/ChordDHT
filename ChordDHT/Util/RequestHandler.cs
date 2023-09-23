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
                Task.Run(() => handlerFunction(context, requestVariables));
                return true;
            };
        }


        public bool HandleRequest(HttpListenerContext context, RequestVariables? variables)
        {
            return (this.listener)(context, variables);
        }


        protected bool SendJsonResponse(HttpListenerContext context, object? result)
        {
            if (!context.Response.OutputStream.CanWrite)
            {
                throw new InvalidOperationException("Response is not writable");
            }
            context.Response.ContentType = "application/json";
            string? response = null;

            if (result == null)
            {
                response = "null";
            }
            else
            {
                switch (result)
                {
                    case string s:
                        response = JsonSerializer.Serialize(s);
                        break;
                    case int i:
                        response = JsonSerializer.Serialize(i);
                        break;
                    case float f:
                        response = JsonSerializer.Serialize(f);
                        break;
                    case double d:
                        response = JsonSerializer.Serialize(d);
                        break;
                    case decimal dm:
                        response = JsonSerializer.Serialize(dm);
                        break;
                    case bool b:
                        response = JsonSerializer.Serialize(b);
                        break;
                    default:
                        try
                        {
                            response = JsonSerializer.Serialize(result);
                        }
                        catch (NotSupportedException)
                        {
                            response = JsonSerializer.Serialize($"Unable to serialize value {result}");
                        };
                        break;
                }
            }
            if (response != null)
            {
                byte[] buffer = Encoding.UTF8.GetBytes(response);
                context.Response.ContentLength64 = buffer.Length;
                context.Response.OutputStream.Write(buffer);
                context.Response.OutputStream.Close();
                return true;
            }
            return false;
        }
    }
}
