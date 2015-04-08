using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KafkaNet.Statistics
{
    public static class StatisticsTracker
    {
     
    }

    public class CapCollection
    {
        private ConcurrentStack<int> _stack;

        public CapCollection()
        {
            
        }
    }
}
