using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace ChordDHT.Util
{
    public class GenericStatusRequestHandler : IRequestHandler
    {
        public HttpStatusCode StatusCode;
        public readonly byte[] StatusMessage;

        public GenericStatusRequestHandler(HttpStatusCode statusCode, byte[] statusMessage)
        {
            StatusCode = statusCode;
            StatusMessage = statusMessage;
        }

        public GenericStatusRequestHandler(int statusCode, string statusMessage)
            : this((HttpStatusCode)statusCode, Encoding.UTF8.GetBytes(statusMessage)) { }

        public GenericStatusRequestHandler(HttpStatusCode statusCode, string statusMessage)
            : this(statusCode, Encoding.UTF8.GetBytes(statusMessage)) { }

        public GenericStatusRequestHandler(int statusCode, byte[] responseBody)
            : this((HttpStatusCode)statusCode, responseBody) { }

        public bool HandleRequest(HttpListenerContext context, RequestVariables? variables = null)
        {
            context.Response.ContentType = "text/plain";
            context.Response.StatusCode = (int) StatusCode;
            context.Response.OutputStream.Write(StatusMessage);
            context.Response.OutputStream.Close();
            return true;
        }
    }
}
