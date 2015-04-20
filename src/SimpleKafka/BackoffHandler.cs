using Serilog;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace SimpleKafka
{
    internal class BackoffHandler
    {
        private static readonly Random generator = new Random();

        private readonly int maxRetries;
        private readonly int basePeriodMs;
        private readonly int jitterMs;
        private int backoffCount;

        public BackoffHandler(int maxRetries = 10, int basePeriodMs = 300, int jitterMs = 50)
        {
            this.maxRetries = maxRetries;
            this.basePeriodMs = basePeriodMs;
            this.jitterMs = jitterMs;
        }

        public async Task<bool> BackoffIfAllowedAsync(CancellationToken token)
        {
            if (++backoffCount >= maxRetries)
            {
                return false;
            }
            else
            {
                Log.Verbose("Backoff {attempt} out of {maxRetries}", backoffCount, maxRetries);
                var delay = generator.Next(basePeriodMs - jitterMs, basePeriodMs + jitterMs);
                await Task.Delay(delay, token).ConfigureAwait(false);
                return true;
            }

        }
    }
}
