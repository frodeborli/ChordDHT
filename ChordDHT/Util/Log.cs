using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ChordDHT.Util
{
    public class Log
    {
        public static void Message(string message)
        {
            Console.WriteLine($"thread={Thread.CurrentThread.ManagedThreadId}: {message}");
        }
    }
}
