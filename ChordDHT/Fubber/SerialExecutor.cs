using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ChordDHT.Fubber
{
    public class SerialExecutor
    {
        private readonly SemaphoreSlim semaphore = new SemaphoreSlim(1, 1);

        public async Task<T> Serial<T>(Func<Task<T>> func)
        {
            await semaphore.WaitAsync();
            try
            {
                return await func();
            }
            finally
            {
                semaphore.Release();
            }
        }

        public async Task Serial(Func<Task> func)
        {
            await semaphore.WaitAsync();
            try
            {
                await func();
            }
            finally
            {
                semaphore.Release();
            }
        }

    }
}
