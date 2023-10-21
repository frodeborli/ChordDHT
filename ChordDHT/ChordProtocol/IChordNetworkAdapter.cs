using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ChordProtocol
{
    public interface IChordNetworkAdapter
    {
        // Method to send a message
        Task<Message> SendMessageAsync(Node receiver, Message message);

        public void SetRequestHandler(Func<Message, Task<Message>> handler);

        // Lifecycle management
        Task StartAsync();
        Task StopAsync();
    }
}
