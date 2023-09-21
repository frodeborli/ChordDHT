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
        protected Func<HttpListenerContext, RequestVariables, bool> listener;


        public RequestHandler(Func<HttpListenerContext, RequestVariables, bool> handlerFunction)
        {
            this.listener = handlerFunction;
        }

        public RequestHandler(Func<HttpListenerContext, bool> handlerFunction)
        {
            this.listener = (context, requestVariables) =>
            {
                return handlerFunction(context);
            };
        }

        public RequestHandler(Func<HttpListenerContext, RequestVariables, object?> handlerFunction)
        {
            this.listener = (context, requestVariables) =>
            {
                object? result = handlerFunction(context, requestVariables);
                return sendJsonResponse(context, result);
            };
        }

        public RequestHandler(Func<HttpListenerContext, object?> handlerFunction)
        {
            this.listener = (context, requestVariables) =>
            {
                object? result = handlerFunction(context);
                return sendJsonResponse(context, result);
            };
        }

        public bool handleRequest(HttpListenerContext context, RequestVariables variables)
        {
            return (this.listener)(context, variables);
        }

        protected bool sendJsonResponse(HttpListenerContext context, object? result)
        {
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
                        catch (JsonException)
                        {

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
