using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ChordDHT.ChordProtocol
{
    interface IChord
    {
        public string Lookup(string key);
    }
}
