﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ChordDHT.DHT
{
    public interface IStoredItem
    {
        public string contentType { get; }
        public byte[] data { get; }
    }
}
