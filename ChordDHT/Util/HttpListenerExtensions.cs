using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace ChordDHT.Util
{
    public static class HttpListenerExtensions
    {
        public static Task<HttpListenerContext> GetContextAsync(this HttpListener listener)
        {
            var tcs = new TaskCompletionSource<HttpListenerContext>();
            listener.BeginGetContext(ar =>
            {
                try
                {
                    var context = listener.EndGetContext(ar);
                    tcs.SetResult(context);
                }
                catch (Exception ex)
                {
                    tcs.SetException(ex);
                }
            }, null);
            return tcs.Task;
        }
    }
}
