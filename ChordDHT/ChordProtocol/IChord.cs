using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ChordDHT.ChordProtocol
{
    interface IChord
    {
        public bool isReady();
        public string lookup(string key);
    }
}
