using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ChordDHT.Fubber
{
    public class SerialExecutor
    {
        /// <summary>
        /// Optimization to avoid trying to lock the semaphore, this value is true whenever a semaphore is locked.
        /// </summary>
        private bool _busy = false;
        private readonly SemaphoreSlim semaphore = new SemaphoreSlim(1, 1);

        /// <summary>
        /// Attempts to execute a function serially with a return value, providing the result if successful.
        /// </summary>
        /// <param name="func">The function to execute.</param>
        /// <param name="result">The result of the function if execution is successful.</param>
        /// <typeparam name="T">The type of the result.</typeparam>
        /// <returns>A Task that returns true if the function was executed, false otherwise.</returns>
        public async Task<(bool, T?)> TrySerial<T>(Func<Task<T>> func)
        {
            if (_busy)
            {
                return (false, default(T));
            }
            bool acquiredLock = await semaphore.WaitAsync(0);
            if (!acquiredLock)
            {
                return (false, default(T));
            }
            try
            {
                _busy = true;
                T result = await func();
                return (true, result);
            }
            finally
            {
                _busy = false;
                semaphore.Release();
            }
            return (false, default(T));
        }

        public async Task<bool> TrySerial(Func<Task> func)
        {
            bool acquiredLock = await semaphore.WaitAsync(0);
            if (!acquiredLock)
            {
                return false;
            }

            try
            {
                _busy = true;
                await func();
                return true;
            }
            finally
            {
                _busy = false;
                semaphore.Release();
            }
        }


        /// <summary>
        /// Executes a function serially with a return value.
        /// </summary>
        /// <param name="func">The function to execute.</param>
        /// <typeparam name="T">The type of the result.</typeparam>
        /// <returns>A Task that returns the result of the function.</returns>
        public async Task<T> Serial<T>(Func<Task<T>> func)
        {
            await semaphore.WaitAsync();
            try
            {
                _busy = true;
                return await func();
            }
            finally
            {
                _busy = false;
                semaphore.Release();
            }
        }

        /// <summary>
        /// Executes a function serially without a return value.
        /// </summary>
        /// <param name="func">The function to execute.</param>
        /// <returns>A Task representing the asynchronous operation.</returns>
        public async Task Serial(Func<Task> func)
        {
            await semaphore.WaitAsync();
            try
            {
                _busy = true;
                await func();
            }
            finally
            {
                _busy = false;
                semaphore.Release();
            }
        }
    }

}
