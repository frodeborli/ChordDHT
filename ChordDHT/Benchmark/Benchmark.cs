using System;
using System.Collections.Concurrent;
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

        public Benchmark(string name, Func<Task<string?>> testFunction, Action? reportFunction = null)
        {
            Name = name;
            TestFunction = testFunction;
            ReportFunction = reportFunction;
        }

        public async Task Run(int repetitions, int workerCount)
        {
            Console.WriteLine($"{Name} {repetitions} repetitions on {workerCount} worker threads");
            Stopwatch sw = new Stopwatch();
            List<Task> tasks = new List<Task>();
            TimeSpan min = TimeSpan.FromSeconds(1000);
            TimeSpan max = TimeSpan.Zero;
            ConcurrentBag<TimeSpan> measurements = new ConcurrentBag<TimeSpan>();
            sw.Start();
            for (int i = 0; i < workerCount; i++)
            {
                tasks.Add(Task.Run(async () => {
                    Stopwatch isw = new Stopwatch();
                    for (int j = 0; j < repetitions; j++)
                    {
                        try
                        {
                            isw.Restart();
                            await TestFunction();
                            isw.Stop();
                            long e = isw.ElapsedTicks;
                            measurements.Add(isw.Elapsed);
                            lock (tasks)
                            {
                                if (min > isw.Elapsed)
                                {
                                    min = isw.Elapsed;
                                }
                                if (max < isw.Elapsed)
                                {
                                    max = isw.Elapsed;
                                }
                            }
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine(ex.ToString());
                        }
                    }
                }));
            }
            await Task.WhenAll(tasks);
            sw.Stop();

            // Calculate Standard Deviation
            double sum = 0;
            int count = measurements.Count;

            // Calculate the mean (average)
            foreach (var timeSpan in measurements)
            {
                sum += timeSpan.TotalMilliseconds;
            }
            double mean = sum / count;

            // Compute the sum of the squared differences from the mean
            double sumOfSquaredDifferences = 0;
            foreach (var timeSpan in measurements)
            {
                double difference = timeSpan.TotalMilliseconds - mean;
                sumOfSquaredDifferences += difference * difference;
            }

            // Calculate the standard deviation
            double standardDeviation = Math.Sqrt(sumOfSquaredDifferences / count);

            // Percentile Calculation
            var sortedMeasurements = measurements.OrderBy(ts => ts.TotalMilliseconds).ToList();


            Console.WriteLine(
                $"RESULTS:\n" +
                $" Repetitions: {measurements.Count}\n" +
                $"  Total time: {sw.Elapsed,6}\n" +
                $"     Minimum: {min,6}\n" +
                $"     Maximum: {max,6}\n" +
                $" Req per sec: {measurements.Count * 1000 / sw.ElapsedMilliseconds}\n" +
                $"          SD: {standardDeviation}\n" +
                $" Percentiles:"
                );
            double[] percentiles = { 99, 98, 95, 75, 50, 25, 10 }; // Add or remove desired percentiles
            foreach (double percentile in percentiles)
            {
                int index = (int)Math.Ceiling((percentile / 100) * count) - 1;
                TimeSpan valueAtPercentile = sortedMeasurements[index];
                Console.WriteLine(
                    $"        {percentile}th: {valueAtPercentile.TotalMilliseconds} milliseconds");
            }
            Console.WriteLine();

            if (ReportFunction != null)
            {
                ReportFunction();
            }
        }
        public async Task RunOld(int repetitions, int workerCount)
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
                            tTotalTime += innerStopwatch.ElapsedMilliseconds;
                            tMinimum = Math.Min(tMinimum, innerStopwatch.ElapsedMilliseconds);
                            tMaximum = Math.Max(tMaximum, innerStopwatch.ElapsedMilliseconds);
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
                                minimumGrouped[group] = Math.Min(minimumGrouped[group], innerStopwatch.ElapsedMilliseconds);
                                maximumGrouped[group] = Math.Max(maximumGrouped[group], innerStopwatch.ElapsedMilliseconds);
                                totalTimeGrouped[group] += innerStopwatch.ElapsedMilliseconds;

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
                $"  Total time: {TimeSpan.FromMilliseconds(totalTime),6}\n" +
                $"     Minimum: {TimeSpan.FromMilliseconds(minimum),6}\n" +
                $"     Maximum: {TimeSpan.FromMilliseconds(maximum),6}\n" +
                $"     Average: {TimeSpan.FromMilliseconds(totalTime / (repetitions * workerCount)),6}\n" +
                $""
                );

            foreach (var key in totalTimeGrouped.Keys)
            {
                Console.WriteLine(
                    $"  GROUPED BY {key} RESULTS:\n" +
                    $"     Instances: {countGrouped[key]}\n" +
                    $"    Total time: {TimeSpan.FromMilliseconds(totalTimeGrouped[key]),6}\n" +
                    $"       Minimum: {TimeSpan.FromMilliseconds(minimumGrouped[key]),6}\n" +
                    $"       Maximum: {TimeSpan.FromMilliseconds(maximumGrouped[key]),6}\n" +
                    $"       Average: {TimeSpan.FromMilliseconds(totalTimeGrouped[key] / countGrouped[key]),6}\n" +
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
