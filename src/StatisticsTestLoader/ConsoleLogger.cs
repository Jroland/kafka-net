using System;
using KafkaNet;

namespace StatisticsTestLoader
{
    public class ConsoleLogger : IKafkaLog
    {
        public void DebugFormat(string format, params object[] args)
        {
            //Console.WriteLine(format, args);
        }

        public void InfoFormat(string format, params object[] args)
        {
            //Console.WriteLine(format, args);
        }

        public void WarnFormat(string format, params object[] args)
        {
            Console.WriteLine(format, args);
        }

        public void ErrorFormat(string format, params object[] args)
        {
            Console.WriteLine(format, args);
        }

        public void FatalFormat(string format, params object[] args)
        {
            Console.WriteLine(format, args);
        }
    }
}