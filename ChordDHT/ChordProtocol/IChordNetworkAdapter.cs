using ChordDHT.ChordProtocol;
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
        Task<TResponseMessage> SendMessageAsync<TRequestMessage, TResponseMessage>(TRequestMessage message)
            where TRequestMessage : IMessage
            where TResponseMessage : IMessage;
        
        public void AddMessageHandler<TRequestMessage, TResponseMessage>(MessageHandler<TRequestMessage, TResponseMessage> handler)
            where TRequestMessage : IMessage
            where TResponseMessage : IMessage;

        public void AddMessageHandler<TRequestMessage, TResponseMessage>(Func<TRequestMessage, Task<TResponseMessage>> handler)
            where TRequestMessage : IMessage
            where TResponseMessage : IMessage;

        // Lifecycle management
        Task StartAsync();
        Task StopAsync();
    }
}
