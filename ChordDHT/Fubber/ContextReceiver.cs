using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Fubber
{
    public class ContextReceiver
    {
        private HttpContext Context;

        public ContextReceiver(HttpContext context)
        {
            Context = context;
        }

        public string FormField(string fieldName)
        {

        }
    }
}
