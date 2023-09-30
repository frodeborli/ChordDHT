using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace ChordDHT.Util
{
    public class StatusCodeResponse : IRequestHandler
    {
        public HttpStatusCode StatusCode;
        public readonly byte[] StatusMessage;

        public StatusCodeResponse(HttpStatusCode statusCode, byte[] statusMessage)
        {
            StatusCode = statusCode;
            StatusMessage = statusMessage;
        }

        public StatusCodeResponse(int statusCode, string responseBody)
            : this((HttpStatusCode) statusCode, Encoding.UTF8.GetBytes(responseBody)) { }

        public bool HandleRequest(HttpListenerContext context, RequestVariables? variables = null)
        {
            context.Response.StatusCode = (int) StatusCode;
            context.Response.OutputStream.Write(StatusMessage);
            context.Response.OutputStream.Close();
            return true;
        }
    }
}
