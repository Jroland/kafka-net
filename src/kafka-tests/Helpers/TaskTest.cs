using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace kafka_tests.Helpers
{
    public static class TaskTest
    {
        public async static Task<bool> WaitFor(Func<bool> predicate, int milliSeconds = 3000)
        {
            var sw = Stopwatch.StartNew();
            while (predicate() != true)
            {
                if (sw.ElapsedMilliseconds > milliSeconds)
                    return false;
                await Task.Delay(100).ConfigureAwait(false);
            }
            return true;
        }
    }
}
