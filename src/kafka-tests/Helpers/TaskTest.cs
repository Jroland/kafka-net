using System;
using System.Diagnostics;
using System.Threading;

namespace kafka_tests.Helpers
{
    public static class TaskTest
    {
        public static bool WaitFor(Func<bool> predicate, int milliSeconds = 3000)
        {
            var sw = Stopwatch.StartNew();
            while (predicate() != true)
            {
                if (sw.ElapsedMilliseconds > milliSeconds)
                    return false;
                Thread.Sleep(100);
            }
            return true;
        }
    }
}
