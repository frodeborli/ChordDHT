using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ChordDHT.Benchmark
{
    public class Benchmark
    {
        public readonly string Name;
        private readonly Func<Task> TestFunction;

        public Benchmark(string name, Func<Task> testFunction)
        {
            Name = name;
            TestFunction = testFunction;
        }

        public async Task Run(int repetitions, int workerCount)
        {
            // Validate input parameters
            if (repetitions <= 0 || workerCount <= 0)
            {
                throw new ArgumentException("Repetitions and workerCount must be greater than zero.");
            }

            Console.WriteLine($"Running benchmark '{Name}' {repetitions} repetitions on {workerCount} worker threads");
            Stopwatch stopwatch = Stopwatch.StartNew();
            long minimum = long.MaxValue;
            long maximum = long.MinValue;

            // Create an array of tasks to run in parallel
            Task[] tasks = new Task[workerCount];

            for (int i = 0; i < workerCount; i++)
            {
                tasks[i] = Task.Run(async () =>
                {
                    Stopwatch innerStopwatch = new Stopwatch();
                    for (int j = 0; j < repetitions; j++)
                    {
                        innerStopwatch.Restart();
                        try
                        {
                            await TestFunction();
                            innerStopwatch.Stop();
                            minimum = Math.Min(minimum, innerStopwatch.ElapsedTicks);
                            maximum = Math.Max(maximum, innerStopwatch.ElapsedTicks);
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine(ex.ToString());
                            innerStopwatch.Stop();
                        }
                    }
                });
            }


            // Wait for all tasks to complete
            await Task.WhenAll(tasks);
            stopwatch.Stop();
            Console.WriteLine(
                $"RESULTS:\n" +
                $" Repetitions: {repetitions * workerCount}\n" +
                $"  Total time: {TimeSpan.FromTicks(stopwatch.ElapsedTicks), 6}\n" +
                $"     Minimum: {TimeSpan.FromTicks(minimum), 6}\n" +
                $"     Maximum: {TimeSpan.FromTicks(maximum), 6}\n" +
                $"     Average: {TimeSpan.FromTicks(stopwatch.ElapsedTicks / (repetitions * workerCount)), 6}\n" +
                $""
                );
        }
    }
}
