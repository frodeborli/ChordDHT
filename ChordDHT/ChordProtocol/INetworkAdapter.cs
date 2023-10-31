using ChordDHT.ChordProtocol;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ChordProtocol
{
    public interface INetworkAdapter
    {

        Task<TResponse> SendMessageAsync<TResponse>(IRequest<TResponse> message)
            where TResponse : IResponse;
        
        /// <summary>
        /// Register a new message handler. The message handler will be invoked based on the TRequestMessage type.
        /// </summary>
        /// <typeparam name="TRequest">The type of the message received that this handler handles</typeparam>
        /// <typeparam name="TResponse">The response type returned by this handler</typeparam>
        /// <param name="handler"></param>
        void AddHandler<TRequest, TResponse>(IRequestHandler<TRequest, TResponse> handler)
            where TRequest : IRequest<TResponse>
            where TResponse : IResponse;

        /// <summary>
        /// A convenience method which automatically creates a MessageHandler instance
        /// </summary>
        /// <typeparam name="TRequest"></typeparam>
        /// <typeparam name="TResponse"></typeparam>
        /// <param name="handler"></param>
        void AddHandler<TRequest, TResponse>(Func<TRequest, Task<TResponse>> handler)
            where TRequest : IRequest<TResponse>
            where TResponse : IResponse;

        /// <summary>
        /// Method will be invoked by the implementation to start the network adapter
        /// </summary>
        /// <returns></returns>
        Task StartAsync();

        /// <summary>
        /// Method will be invoked by the implementation to stop the network adapter
        /// </summary>
        /// <returns></returns>
        Task StopAsync();
    }
}
