using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace ChordDHT.Util
{
    public interface IRequestHandler
    {
        /**
         * Function that will handle the request and provide a response.
         * If the function handles the response it should return true,
         * and if the function does not handle the response it should
         * return false.
         */
        public bool handleRequest(HttpListenerContext context, RequestVariables variables);
    }
}
