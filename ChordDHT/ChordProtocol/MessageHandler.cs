using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ChordDHT.ChordProtocol
{
    public class MessageHandler<TRequestMessage, TResponseMessage> : IMessageHandler
        where TRequestMessage : IMessage
        where TResponseMessage : IMessage
    {
        private readonly Func<TRequestMessage, Task<TResponseMessage>> _handlerFunc;

        public MessageHandler(Func<TRequestMessage, Task<TResponseMessage>> handlerFunc)
        {
            _handlerFunc = handlerFunc;
        }

        public async Task<IMessage> HandleMessageAsync(IMessage message)
        {
            if (message is TRequestMessage receivedMessage)
            {
                return await _handlerFunc(receivedMessage);
            }

            throw new InvalidOperationException($"No handlers for the message type {message.GetType().Name}");
        }
    }

}
