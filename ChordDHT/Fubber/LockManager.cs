using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ChordDHT.Fubber
{
    public class LockManager
    {
        private int UpdateLock = 0;
        private ConcurrentQueue<TaskCompletionSource<bool>> awaiters = new ConcurrentQueue<TaskCompletionSource<bool>>();

        public bool Acquire()
        {
            return Interlocked.CompareExchange(ref UpdateLock, 1, 0) == 0;
        }

        public bool Acquire(TimeSpan timeout) => Task.Run(() => AcquireAsync(timeout)).Result;

        public async Task<bool> AcquireAsync(TimeSpan timeout)
        {
            TaskCompletionSource<bool> tcs = new TaskCompletionSource<bool>();

            if (Acquire())
            {
                return true;
            }

            awaiters.Enqueue(tcs);

            if (await Task.WhenAny(tcs.Task, Task.Delay(timeout)) == tcs.Task && tcs.Task.Result)
            {
                return true;
            }

            // Remove the task from the queue if timeout occurs.
            TaskCompletionSource<bool> ignored;
            awaiters.TryDequeue(out ignored);

            return false;
        }

        public void Release()
        {
            Interlocked.Exchange(ref UpdateLock, 0);

            if (awaiters.TryDequeue(out TaskCompletionSource<bool> tcs))
            {
                tcs.SetResult(true);
            }
        }
    }

}
