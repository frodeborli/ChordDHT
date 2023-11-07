using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ChordDHT.Fubber
{
    public class ObjectPool<T>
        where T : ObjectPool<T>.IPoolable
    {
        public interface IPoolable
        {
            public T CreateInstance();
        }
    }


}
