using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ChordDHT.ChordProtocol
{
    public class RequestHandler<TRequest, TResponse> : IRequestHandler<TRequest, TResponse>
        where TRequest : IRequest<TResponse>
        where TResponse : IResponse
    {
        private readonly Func<TRequest, Task<TResponse>> _handlerFunc;

        public RequestHandler(Func<TRequest, Task<TResponse>> handlerFunc)
        {
            _handlerFunc = handlerFunc;
        }
        
        public RequestHandler(Func<TRequest, TResponse> handlerFunc)
        {
            _handlerFunc = (request) => Task.FromResult(handlerFunc(request));
        }

        public Task<TResponse> HandleMessageAsync(TRequest message)
        {
            return _handlerFunc(message);
        }

        public TResponse EnsureType(IResponse response)
        {
            if (response is TResponse typedResponse)
            {
                return typedResponse;
            }
            throw new InvalidOperationException($"Expecting type {typeof(TResponse)} but got {response.GetType()}");
        }

        public async Task<IResponse> HandleMessageAsync(IGenericRequest message)
        {
            if (message is TRequest typedRequest)
            {
                var result = await _handlerFunc(typedRequest);
                return result;
            }
            throw new InvalidOperationException($"Trying to use a request handler generic for {message.GetType().Name} in the handler for {typeof(TRequest)} and {typeof(TResponse)}");
        }
    }

}
