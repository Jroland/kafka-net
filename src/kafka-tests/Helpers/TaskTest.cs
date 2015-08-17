using System;
using System.Diagnostics;
using System.Threading.Tasks;

namespace kafka_tests.Helpers
{
    public static class TaskTest
    {
        /// <exception cref="Exception">A delegate callback throws an exception.</exception>
        public async static Task<bool> WaitFor(Func<bool> predicate, int milliSeconds = 3000)
        {
            var sw = Stopwatch.StartNew();
            while (predicate() == false)
            {
                if (sw.ElapsedMilliseconds > milliSeconds)
                    return false;
                await Task.Delay(50).ConfigureAwait(false);
            }
            return true;
        }
    }
}