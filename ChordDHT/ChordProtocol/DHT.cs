using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ChordDHT.ChordProtocol
{
    public class DHT
    {
        protected Chord chord;
        public string hostname;
        public int port;

        public DHT(string hostname, int port) {
            this.hostname = hostname;
            this.port = port;
            this.chord = new Chord($"{hostname}:{port}");
        }


    }
}
