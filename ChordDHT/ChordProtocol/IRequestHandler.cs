using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ChordDHT.ChordProtocol
{
    public interface IRequestHandler<TRequest, TResponse> : IGenericRequestHandler
        where TRequest : IRequest<TResponse>
        where TResponse : IResponse
    {
        public Task<TResponse> HandleMessageAsync(TRequest request);
    }

    public interface IGenericRequestHandler
    {
        public Task<IResponse> HandleMessageAsync(IGenericRequest request);
    }
}
