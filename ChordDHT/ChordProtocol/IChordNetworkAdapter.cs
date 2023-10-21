using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ChordProtocol
{
    public interface IChordNetworkAdapter
    {
        // Event to notify when a message is received
        event EventHandler<Message>? MessageReceived;

        // Method to send a message
        Task<Message> SendMessageAsync(Node sender, Node receiver, Message message);

        // Lifecycle management
        Task StartAsync();
        Task StopAsync();
    }
}
