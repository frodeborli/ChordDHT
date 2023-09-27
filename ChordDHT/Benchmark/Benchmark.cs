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
        private readonly Func<Task<string?>> TestFunction;
        private readonly Action? ReportFunction;

        public Benchmark(string name, Func<Task<string?>> testFunction, Action? reportFunction=null)
        {
            Name = name;
            TestFunction = testFunction;
            ReportFunction = reportFunction;
        }

        public async Task Run(int repetitions, int workerCount)
        {
            // Validate input parameters
            if (repetitions <= 0 || workerCount <= 0)
            {
                throw new ArgumentException("Repetitions and workerCount must be greater than zero.");
            }

            Console.WriteLine($"Running benchmark '{Name}' {repetitions} repetitions on {workerCount} worker threads");
            long minimum = long.MaxValue;
            long maximum = long.MinValue;
            long totalTime = 0;
            object lockObject = new object();
            Dictionary<string, long> countGrouped = new Dictionary<string, long>();
            Dictionary<string, long> minimumGrouped = new Dictionary<string, long>();
            Dictionary<string, long> maximumGrouped = new Dictionary<string, long>();
            Dictionary<string, long> totalTimeGrouped = new Dictionary<string, long>();

            // Create an array of tasks to run in parallel
            Task[] tasks = new Task[workerCount];

            for (int i = 0; i < workerCount; i++)
            {
                tasks[i] = Task.Run(async () =>
                {
                    long tMinimum = long.MaxValue;
                    long tMaximum = long.MinValue;
                    long tTotalTime = 0;
                    Stopwatch innerStopwatch = new Stopwatch();
                    for (int j = 0; j < repetitions; j++)
                    {
                        string? group;
                        try
                        {
                            innerStopwatch.Restart();
                            group = await TestFunction();
                            innerStopwatch.Stop();
                            tTotalTime += innerStopwatch.ElapsedTicks;
                            tMinimum = Math.Min(tMinimum, innerStopwatch.ElapsedTicks);
                            tMaximum = Math.Max(tMaximum, innerStopwatch.ElapsedTicks);
                            await Task.Yield();
                            lock (totalTimeGrouped)
                            {
                                if (!minimumGrouped.ContainsKey(group))
                                {
                                    countGrouped[group] = 0;
                                    minimumGrouped[group] = long.MaxValue;
                                    maximumGrouped[group] = long.MinValue;
                                    totalTimeGrouped[group] = 0;
                                }
                                countGrouped[group]++;
                                minimumGrouped[group] = Math.Min(minimumGrouped[group], innerStopwatch.ElapsedTicks);
                                maximumGrouped[group] = Math.Max(maximumGrouped[group], innerStopwatch.ElapsedTicks);
                                totalTimeGrouped[group] += innerStopwatch.ElapsedTicks;
                            }
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine(ex.ToString());
                            innerStopwatch.Stop();
                        }
                    }
                    lock (lockObject)
                    {
                        minimum = Math.Min(tMinimum, minimum);
                        maximum = Math.Max(tMaximum, maximum);
                        totalTime += tTotalTime;
                    }
                });
            }


            // Wait for all tasks to complete
            await Task.WhenAll(tasks);
            Console.WriteLine(
                $"RESULTS:\n" +
                $" Repetitions: {repetitions * workerCount}\n" +
                $"  Total time: {TimeSpan.FromTicks(totalTime), 6}\n" +
                $"     Minimum: {TimeSpan.FromTicks(minimum), 6}\n" +
                $"     Maximum: {TimeSpan.FromTicks(maximum), 6}\n" +
                $"     Average: {TimeSpan.FromTicks(totalTime / (repetitions * workerCount)), 6}\n" +
                $""
                );

            foreach (var key in totalTimeGrouped.Keys)
            {
                Console.WriteLine(
                    $"  GROUPED BY {key} RESULTS:\n" +
                    $"     Instances: {countGrouped[key]}\n" +
                    $"    Total time: {TimeSpan.FromTicks(totalTimeGrouped[key]),6}\n" +
                    $"       Minimum: {TimeSpan.FromTicks(minimumGrouped[key]),6}\n" +
                    $"       Maximum: {TimeSpan.FromTicks(maximumGrouped[key]),6}\n" +
                    $"       Average: {TimeSpan.FromTicks(totalTimeGrouped[key] / countGrouped[key]),6}\n" +
                    $""
                    );
            }
            if (ReportFunction != null)
            {
                ReportFunction();
            }
        }
    }
}
