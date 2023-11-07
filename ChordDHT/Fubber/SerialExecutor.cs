using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ChordDHT.Fubber
{
    public class SerialExecutor
    {
        private StackTrace? _currentLockHolder = null;

        /// <summary>
        /// Optimization to avoid trying to lock the semaphore, this value is true whenever a semaphore is locked.
        /// </summary>
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
            bool acquiredLock = await semaphore.WaitAsync(0);
            if (!acquiredLock)
            {
                return (false, default(T));
            }
            _currentLockHolder = new StackTrace();
            try
            {
                T result = await func();
                return (true, result);
            }
            finally
            {
                _currentLockHolder = null;
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
            _currentLockHolder = new StackTrace();
            try
            {
                await func();
                return true;
            }
            finally
            {
                _currentLockHolder = null;
                semaphore.Release();
            }
        }

        public async Task<bool> TrySerial(Action func)
        {
            bool acquiredLock = await semaphore.WaitAsync(0);
            if (!acquiredLock)
            {
                return false;
            }
            _currentLockHolder = new StackTrace();
            try
            {
                func();
                return true;
            }
            finally
            {
                _currentLockHolder = null;
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
            var locked = await semaphore.WaitAsync(15);
            if (!locked)
            {
                Console.WriteLine($"1 FAILED GETTING LOCK FOR RUNNING {func}\n - locked by {_currentLockHolder}");
                throw new Exception($"Unable to get a lock for running {func}\n - locked by {_currentLockHolder}");
            }
            _currentLockHolder = new StackTrace();
            try
            {
                return await func();
            }
            finally
            {
                _currentLockHolder = null;
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
            var locked = await semaphore.WaitAsync(15);
            if (!locked)
            {
                Console.WriteLine($"2 FAILED GETTING LOCK FOR RUNNING {func}\n - locked by {_currentLockHolder}");
                throw new Exception($"Unable to get a lock for running {func}\n - locked by {_currentLockHolder}");
            }
            _currentLockHolder = new StackTrace();
            try
            {
                await func();
            }
            finally
            {
                _currentLockHolder = null;
                semaphore.Release();
            }
        }
    }

}
