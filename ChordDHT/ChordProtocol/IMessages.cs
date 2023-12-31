﻿using ChordProtocol;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace ChordDHT.ChordProtocol
{

    public interface IMessage
    {
        public string ToString();
    }

    public interface IResponse : IMessage
    {
    }

    public interface IRequest<TResponse> : IGenericRequest
        where TResponse : IResponse
    {
        public Node? Sender { get; set; }

        /// <summary>
        /// Function receives the response and may validate the response.
        /// </summary>
        /// <param name="response"></param>
        /// <returns></returns>
        public TResponse Filter(TResponse response);
    }

    public interface IGenericRequest : IMessage { }

}
